# Chat API Enhancements: Cancel, Edit, Retry, and Rating

## Overview
New features added to the Athena Chat API for better conversation management and AI response quality control.

## Features

### 1. Cancel/Stop Active Requests
Cancel an in-progress completion request.

**Endpoint:** `POST /conversations/{conversation_id}/completions/{request_id}/cancel`

**Use Case:** Stop a long-running or incorrect response generation.

**Request:**
```json
POST /conversations/abc123/completions/def456/cancel
```

**Response:**
```json
{
  "cancelled": true,
  "request_id": "def456"
}
```

**Notes:**
- The `request_id` is returned in the completion response
- Only running requests can be cancelled
- User must be a member of the conversation's space

---

### 2. Edit and Resend Messages
Edit a user message and optionally regenerate the assistant's response.

**Endpoint:** `PUT /messages/{message_id}/edit`

**Use Case:** Fix typos or clarify a question, then get a new response.

**Request:**
```json
{
  "content": "What is the weather in Seattle?",
  "regenerate": true,
  "quality": "high",
  "edit_reason": "Fixed typo in city name"
}
```

**Response:**
```json
{
  "message": {
    "id": "msg789",
    "conversation_id": "abc123",
    "role": "user",
    "content_text": "What is the weather in Seattle?",
    "edited_from": "msg456",
    "quality_tier": "high",
    ...
  },
  "edited": true,
  "regenerating": true
}
```

**Parameters:**
- `content` (required) — New message text
- `regenerate` (optional, default: true) — Whether to regenerate assistant response
- `quality` (optional, default: "auto") — Quality tier for regeneration
- `edit_reason` (optional) — Reason for the edit (stored in audit trail)

**Notes:**
- Only user messages can be edited
- Edit history is tracked in `chat.message_edits` table
- Subsequent assistant messages are deleted when regenerating

---

### 3. Retry with Quality Adjustment
Retry generating a response with a different quality tier.

**Endpoint:** `POST /messages/{message_id}/retry`

**Use Case:** Regenerate an unsatisfactory response with higher quality settings.

**Request:**
```json
{
  "quality": "xhigh",
  "delete_previous": true
}
```

**Response:**
```json
{
  "retrying": true,
  "conversation_id": "abc123",
  "quality": "xhigh",
  "message": "Previous message deleted. Call /conversations/{id}/completions to regenerate."
}
```

**Parameters:**
- `quality` (optional, default: "auto") — Quality tier for retry
  - `auto` — Default balanced quality
  - `low` — Faster, lower reasoning effort
  - `med` — Medium reasoning effort
  - `high` — High reasoning effort
  - `xhigh` — Maximum reasoning effort
- `delete_previous` (optional, default: true) — Delete the previous assistant message

**Notes:**
- Only assistant messages can be retried
- After calling retry, you must call the completions endpoint to regenerate
- Quality tiers affect reasoning effort and token limits

---

### 4. Rate Responses
Rate assistant messages for quality feedback and AI training.

**Endpoint:** `POST /messages/{message_id}/rate`

**Use Case:** Provide feedback on response quality to improve AI performance.

**Request:**
```json
{
  "rating": 5,
  "rating_type": "helpfulness",
  "feedback": "Very accurate and comprehensive answer!",
  "metadata": {
    "context": "weather query",
    "response_time": "fast"
  }
}
```

**Response:**
```json
{
  "id": 42,
  "message_id": "msg789",
  "user_id": 1,
  "rating": 5,
  "rating_type": "helpfulness",
  "feedback_text": "Very accurate and comprehensive answer!",
  "metadata": {...},
  "created_at": "2026-02-09T14:30:00Z",
  "updated_at": "2026-02-09T14:30:00Z"
}
```

**Parameters:**
- `rating` (required) — Integer rating from 1-5
  - 1 = Poor
  - 2 = Below Average
  - 3 = Average
  - 4 = Good
  - 5 = Excellent
- `rating_type` (optional, default: "overall") — Type of rating
  - `accuracy` — Factual correctness
  - `helpfulness` — How helpful the response was
  - `quality` — Overall quality of writing/structure
  - `overall` — General satisfaction
- `feedback` (optional) — Text feedback explaining the rating
- `metadata` (optional) — Additional structured data

**Get Ratings:**
`GET /messages/{message_id}/ratings`

Returns all ratings for a message.

**Notes:**
- One rating per user per rating_type per message
- Ratings can be updated by the same user
- Useful for training data and quality monitoring

---

## Quality Tiers

Quality tiers control the model's reasoning effort and response quality:

| Tier  | Model   | Max Tokens | Reasoning Effort | Use Case |
|-------|---------|------------|------------------|----------|
| auto  | gpt-5.2 | 16384      | None             | Balanced performance |
| low   | gpt-5.2 | 8192       | low              | Quick responses |
| med   | gpt-5.2 | 16384      | medium           | Standard quality |
| high  | gpt-5.2 | 16384      | high             | Complex tasks |
| xhigh | gpt-5.2 | 16384      | xhigh            | Maximum quality |

**Get Available Tiers:**
`GET /quality-tiers`

---

## Database Schema

### New Tables

**`chat.completion_requests`**
Tracks active completion requests for cancellation support.
```sql
- id (UUID, PK)
- conversation_id (UUID, FK)
- user_id (BIGINT, FK)
- status (running|completed|cancelled|failed)
- quality_tier (TEXT)
- model (TEXT)
- message_count (INT)
- started_at (TIMESTAMPTZ)
- completed_at (TIMESTAMPTZ)
- error_message (TEXT)
```

**`chat.message_ratings`**
Stores user ratings of assistant messages.
```sql
- id (BIGSERIAL, PK)
- message_id (UUID, FK)
- user_id (BIGINT, FK)
- rating (INT, 1-5)
- rating_type (accuracy|helpfulness|quality|overall)
- feedback_text (TEXT)
- metadata (JSONB)
- created_at (TIMESTAMPTZ)
- updated_at (TIMESTAMPTZ)
- UNIQUE(message_id, user_id, rating_type)
```

**`chat.message_edits`**
Audit trail for message edits.
```sql
- id (BIGSERIAL, PK)
- message_id (UUID, FK)
- previous_content (TEXT)
- new_content (TEXT)
- edited_by (BIGINT, FK)
- edit_reason (TEXT)
- created_at (TIMESTAMPTZ)
```

### Updated Columns

**`chat.messages`**
Added columns:
- `edited_from` (UUID) — Original message ID if this is an edit
- `retry_of` (UUID) — Message ID if this is a retry
- `quality_tier` (TEXT) — Quality tier used for this message

---

## Example Workflows

### Workflow 1: Edit and Regenerate
```bash
# 1. Edit a typo in user message
PUT /messages/msg123/edit
{
  "content": "What is the capital of France?",
  "regenerate": true,
  "quality": "high"
}

# 2. Get new completion (automatic if regenerate=true)
# The API deletes old assistant responses and generates a new one

# 3. Rate the new response
POST /messages/msg456/rate
{
  "rating": 5,
  "rating_type": "accuracy"
}
```

### Workflow 2: Retry with Higher Quality
```bash
# 1. User receives a low-quality response
# message_id: msg789

# 2. Retry with higher quality
POST /messages/msg789/retry
{
  "quality": "xhigh",
  "delete_previous": true
}

# 3. Generate new completion
POST /conversations/abc123/completions
{
  "quality": "xhigh"
}

# Response includes request_id for cancellation
```

### Workflow 3: Cancel Long Request
```bash
# 1. Start a complex completion
POST /conversations/abc123/completions
{
  "message": "Write a detailed analysis...",
  "stream": true,
  "quality": "xhigh"
}

# Response: { "request_id": "req123" }

# 2. User changes their mind
POST /conversations/abc123/completions/req123/cancel

# Stream will stop and return { "cancelled": true }
```

---

## Migration

Applied via: `/opt/wdws/migrations/007_chat_enhancements.sql`

To apply:
```bash
sudo -u postgres psql athena_chat -f /opt/wdws/migrations/007_chat_enhancements.sql
```

---

## Testing

Test the endpoints with curl:

```bash
# Set variables
export CONV_ID="your-conversation-id"
export MSG_ID="your-message-id"
export API_URL="http://localhost:9350"

# Rate a message
curl -X POST "$API_URL/messages/$MSG_ID/rate" \
  -H "Content-Type: application/json" \
  -d '{"rating": 5, "rating_type": "overall", "feedback": "Great response!"}'

# Edit a message
curl -X PUT "$API_URL/messages/$MSG_ID/edit" \
  -H "Content-Type: application/json" \
  -d '{"content": "New message text", "regenerate": true, "quality": "high"}'

# Retry a message
curl -X POST "$API_URL/messages/$MSG_ID/retry" \
  -H "Content-Type: application/json" \
  -d '{"quality": "xhigh"}'
```

---

## Notes

- All endpoints require the user to be a space member
- Request tracking adds minimal overhead (~1ms per completion)
- Cancellation checks occur on each stream chunk (efficient)
- Edit history is preserved for audit purposes
- Ratings are stored per user per type (can have multiple rating types per message)
- Quality tiers match the dashboard UI at http://172.16.32.207:9100
