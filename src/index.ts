import path from 'path';
import { connectToTelegram } from './config/tg';
import { runTgCrawler } from './services/tg.services';
require('dotenv').config({ path: path.resolve(__dirname, '../.env') });

async function start() {
  await connectToTelegram();
  await runTgCrawler();
  console.log('Crawling complete, shutting down server');
  process.exit(0);
}

start().catch((e) => {
  console.error(e);
  process.exit(1);
});
