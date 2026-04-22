using System;
using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.InformationProtection;
using Microsoft.InformationProtection.File;

// Athena Purview Decrypt
// Usage: purview-decrypt <input.rpmsg> <output.msg>
//
// Env vars required:
//   PURVIEW_TENANT_ID
//   PURVIEW_CLIENT_ID
//   PURVIEW_CLIENT_SECRET
//
// Output JSON to stderr (for Python wrapper to parse), decrypted bytes to output path.

class AuthDelegate : IAuthDelegate
{
    // Dual-mode auth:
    //  - aadrm.com  → delegated token (via `az` CLI) acting as the signed-in user.
    //                 If env var RMS_TARGET_TENANT is set, route through that
    //                 tenant's STS — needed when the PL was issued by a foreign
    //                 tenant that recognizes this user as the recipient.
    //  - everything else (syncservice.o365syncservice.com) → app-only client creds
    //                 against our registered app.
    public string AcquireToken(Identity identity, string authority, string resource, string claims)
    {
        Console.Error.WriteLine($"[auth] identity={identity?.Email} resource={resource}");

        if (resource.Contains("aadrm.com"))
        {
            return AcquireDelegated(resource);
        }
        return AcquireAppOnly(resource);
    }

    string AcquireDelegated(string resource)
    {
        var targetTenant = Environment.GetEnvironmentVariable("RMS_TARGET_TENANT");
        var tenantArg = !string.IsNullOrEmpty(targetTenant) ? $" --tenant {targetTenant}" : "";

        var psi = new System.Diagnostics.ProcessStartInfo("az",
            $"account get-access-token --resource {resource}{tenantArg} --query accessToken -o tsv")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        using var p = System.Diagnostics.Process.Start(psi)!;
        var token = p.StandardOutput.ReadToEnd().Trim();
        p.WaitForExit();
        if (p.ExitCode != 0 || string.IsNullOrEmpty(token))
        {
            var err = p.StandardError.ReadToEnd();
            throw new Exception($"az delegated token failed (tenant={targetTenant}): {err}");
        }
        return token;
    }

    string AcquireAppOnly(string resource)
    {
        var tenantId = Environment.GetEnvironmentVariable("PURVIEW_TENANT_ID");
        var clientId = Environment.GetEnvironmentVariable("PURVIEW_CLIENT_ID");
        var clientSecret = Environment.GetEnvironmentVariable("PURVIEW_CLIENT_SECRET");
        var scope = resource.TrimEnd('/') + "/.default";

        using var http = new HttpClient();
        var body = new FormUrlEncodedContent(new[]
        {
            new KeyValuePair<string,string>("client_id", clientId!),
            new KeyValuePair<string,string>("client_secret", clientSecret!),
            new KeyValuePair<string,string>("scope", scope),
            new KeyValuePair<string,string>("grant_type", "client_credentials"),
        });
        var resp = http.PostAsync($"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token", body).Result;
        var txt = resp.Content.ReadAsStringAsync().Result;
        if (!resp.IsSuccessStatusCode)
            throw new Exception($"app-only token failed ({(int)resp.StatusCode}): {txt}");
        using var doc = JsonDocument.Parse(txt);
        return doc.RootElement.GetProperty("access_token").GetString()!;
    }
}

class ConsentDelegate : IConsentDelegate
{
    public Consent GetUserConsent(string url) => Consent.Accept;
}

class Program
{
    static async Task<int> Main(string[] args)
    {
        if (args.Length != 2)
        {
            Console.Error.WriteLine("usage: purview-decrypt <input.rpmsg> <output.msg>");
            return 2;
        }
        var input = args[0];
        var output = args[1];

        try
        {
            var appInfo = new ApplicationInfo
            {
                ApplicationId = Environment.GetEnvironmentVariable("PURVIEW_CLIENT_ID"),
                ApplicationName = "AthenaPurviewDecrypt",
                ApplicationVersion = "1.0.0",
            };
            var authDelegate = new AuthDelegate();
            var consentDelegate = new ConsentDelegate();

            var mipContext = MIP.CreateMipContext(
                appInfo,
                "/tmp/mip_state",
                LogLevel.Info,
                null,
                null);

            var profileSettings = new FileProfileSettings(
                mipContext,
                CacheStorageType.InMemory,
                consentDelegate);
            var fileProfile = await MIP.LoadFileProfileAsync(profileSettings);

            var identity = new Identity($"{Environment.GetEnvironmentVariable("PURVIEW_CLIENT_ID")}@{Environment.GetEnvironmentVariable("PURVIEW_TENANT_ID")}");
            var engineSettings = new FileEngineSettings("athena-purview-decrypt", authDelegate, "", "en-US")
            {
                Identity = identity,
                // Enables the MsgInspector path for .rpmsg / .msg files. Without
                // this flag the File API refuses to recognize RPMsg as inspectable.
                CustomSettings = new List<KeyValuePair<string, string>>
                {
                    new KeyValuePair<string, string>("enable_msg_file_type", "true"),
                },
            };
            var fileEngine = await fileProfile.AddEngineAsync(engineSettings);

            // Probe the PL first so we know the issuing tenant and can route the
            // AuthDelegate there BEFORE the File API's own consumption call happens.
            System.Collections.Generic.List<byte>? plList = null;
            try
            {
                using var fs0 = System.IO.File.OpenRead(input);
                plList = Microsoft.InformationProtection.File.FileHandler.GetSerializedPublishingLicense(
                    fs0, input, mipContext, null);
            }
            catch { /* fall through — if PL extraction fails we'll fail below */ }

            string? plIssuer = null;
            string? plContentId = null;
            if (plList != null && plList.Count > 0)
            {
                var plInfo = Microsoft.InformationProtection.Protection.PublishingLicenseInfo.GetPublishingLicenseInfo(plList);
                plIssuer = plInfo.Owner;
                plContentId = plInfo.ContentId;
                Console.Error.WriteLine($"[info] PL owner: {plIssuer}, ContentId: {plContentId}");

                // Peek the issuing tenant by doing a one-shot consumption with
                // app-only creds. We expect this to fail with issuerPrefix=<tenant>
                // for foreign-issued content. Use that to pre-set RMS_TARGET_TENANT
                // so the real File API call routes auth through the right tenant.
                var protectionProfileSettings = new Microsoft.InformationProtection.Protection.ProtectionProfileSettings(
                    mipContext, CacheStorageType.InMemory, consentDelegate);
                var protectionProfile = await Microsoft.InformationProtection.MIP.LoadProtectionProfileAsync(protectionProfileSettings);
                var protectionEngineSettings = new Microsoft.InformationProtection.Protection.ProtectionEngineSettings(
                    "athena-purview-decrypt", authDelegate, "", "en-US")
                { Identity = identity };
                var protectionEngine = await protectionProfile.AddEngineAsync(protectionEngineSettings);

                try
                {
                    var cs = new Microsoft.InformationProtection.Protection.ConsumptionSettings(plInfo);
                    await protectionEngine.CreateProtectionHandlerForConsumptionAsync(cs);
                }
                catch (Exception e) when (e.Message.Contains("issuerPrefix"))
                {
                    var m = System.Text.RegularExpressions.Regex.Match(
                        e.Message, @"issuerPrefix:https://sts\.windows\.net/([0-9a-f-]+)");
                    if (m.Success)
                    {
                        Environment.SetEnvironmentVariable("RMS_TARGET_TENANT", m.Groups[1].Value);
                        Console.Error.WriteLine($"[info] routing auth via foreign tenant {m.Groups[1].Value}");
                    }
                }
                catch { /* other errors: keep going and let main flow surface them */ }
            }

            // The File API refuses to decrypt .rpmsg to a file, but WILL inspect
            // it when `enable_msg_file_type` is set. The inspector exposes the
            // decrypted body + attachments as byte streams.
            using var input2 = System.IO.File.OpenRead(input);
            var fileHandler = await fileEngine.CreateFileHandlerAsync(
                input2, input,
                /*isAuditDiscoveryEnabled*/ false,
                /*executionState*/ null,
                /*contentIsEncrypted*/ true);

            // Try InspectAsync first (works for .rpmsg/.msg via MsgInspector).
            // If the file type isn't inspectable (e.g. AIP-protected PDFs, Office
            // docs), fall back to GetDecryptedTemporaryFileAsync which is the
            // File API's primary decrypt-to-file path.
            try
            {
                var inspector = await fileHandler.InspectAsync();
                Console.Error.WriteLine($"[info] inspector.Type={inspector.Type}");

                if (inspector.Type == Microsoft.InformationProtection.File.InspectorType.Msg)
                {
                    var msgInspector = (Microsoft.InformationProtection.File.IMsgInspector)inspector;
                    var bodyBytes = msgInspector.Body.ToArray();

                    var outDir = Path.GetDirectoryName(output) ?? ".";
                    var baseName = Path.GetFileNameWithoutExtension(output);
                    File.WriteAllBytes(output, bodyBytes);

                    var attachmentInfo = new List<object>();
                    int idx = 0;
                    foreach (var att in msgInspector.Attachments)
                    {
                        var attOut = Path.Combine(outDir, $"{baseName}.att{idx:D2}_{att.Name}");
                        File.WriteAllBytes(attOut, att.Bytes.ToArray());
                        attachmentInfo.Add(new {
                            index = idx,
                            name = att.Name,
                            longName = att.LongName,
                            size = att.Bytes.Count,
                            output = attOut,
                        });
                        idx++;
                    }

                    Console.WriteLine(JsonSerializer.Serialize(new {
                        status = "ok",
                        mode = "msg_inspector",
                        input, output,
                        plOwner = plIssuer,
                        bodyType = msgInspector.BodyType.ToString(),
                        codePage = msgInspector.CodePage,
                        bodySize = bodyBytes.Length,
                        attachmentCount = attachmentInfo.Count,
                        attachments = attachmentInfo,
                    }));
                    return 0;
                }
            }
            catch (Exception ie) when (ie.Message.Contains("cannot be inspected"))
            {
                Console.Error.WriteLine("[info] InspectAsync unsupported, falling back to decrypt-to-file");
            }

            // Fallback: decrypt-to-file path for PDF/Office/.pfile protected content.
            var tmp = await fileHandler.GetDecryptedTemporaryFileAsync();
            File.Copy(tmp, output, overwrite: true);
            File.Delete(tmp);
            Console.WriteLine(JsonSerializer.Serialize(new {
                status = "ok",
                mode = "decrypt_to_file",
                input, output,
                plOwner = plIssuer,
                outputSize = new FileInfo(output).Length,
            }));
            return 0;

        }
        catch (Exception ex)
        {
            Console.WriteLine(JsonSerializer.Serialize(new {
                status = "error",
                error = ex.Message,
                type = ex.GetType().Name,
                stack = ex.StackTrace,
            }));
            return 1;
        }
    }
}
