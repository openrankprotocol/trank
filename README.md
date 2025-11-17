# Telegram Channel Crawler

A Node.js tool to fetch and archive messages from Telegram channels.

## Features

- Crawls multiple Telegram channels simultaneously
- Time-window based message filtering (default: last 3 days)
- Rate limiting and parallel processing
- Exports messages to JSON files

## Setup

1. Get Telegram API credentials from https://my.telegram.org

2. Install dependencies:
```bash
npm install
```

3. Create `.env` file:
```env
TELEGRAM_APP_ID=your_app_id
TELEGRAM_APP_HASH=your_app_hash
TELEGRAM_APP_SESSION=your_session_string
```

4. Generate session string:
```bash
npm run login
```

5. Copy the generated session string to `.env`

## Configuration

Edit `src/config/tg.ts` to configure:
- `channels` - List of channel usernames or IDs to crawl
- `timeWindowDays` - How many days back to fetch (default: 3)
- `maxMessagesPerChannel` - Message limit per channel (default: 2000)
- `parallelRequests` - Number of channels to process concurrently (default: 3)
- `rateLimitingDelay` - Delay between requests in ms (default: 500)

## Usage

```bash
# Development
npm run dev

# Production
npm run build
npm start
```

## Output

Messages are saved as JSON files in the project root, named by channel (e.g., `thechaoschain.json`, `-1001772588611.json`).

## Scripts

- `npm run dev` - Run in development mode
- `npm run build` - Compile TypeScript
- `npm start` - Run compiled application
- `npm run login` - Generate new session
- `npm run format` - Format code with Prettier
