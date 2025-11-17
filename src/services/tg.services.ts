import { Api } from 'telegram';
import { AppTgConfig, TgClient } from '../config/tg';
import fs from 'fs';
import { chunkArray, delay } from '../utils/misc';

export async function fetchChannelMessages(channel: string | number) {
  const sinceUnix = Math.floor((Date.now() - AppTgConfig.timeWindowDays * 86400000) / 1000);
  let offsetId = 0;
  const result: Api.Message[] = [];

  while (result.length < AppTgConfig.maxMessagesPerChannel) {
    await delay(AppTgConfig.rateLimitingDelay);
    const history = await TgClient.invoke(
      new Api.messages.GetHistory({
        peer: channel,
        offsetId,
        limit: 100,
      }),
    );

    const messages = history.className === 'messages.ChannelMessages' ? history.messages || [] : [];

    offsetId = messages[messages.length - 1].id;

    if (messages.length < 1) break;

    for (const msg of messages) {
      let m = msg as any;
      if (!m.date) {
        continue;
      }
      if (m.date < sinceUnix) return { channel, result };
      result.push(m);
    }
  }

  return { channel, result };
}

export async function runTgCrawler() {
  for (const chunk of chunkArray(AppTgConfig.channels, AppTgConfig.parallelRequests)) {
    const res = await Promise.allSettled(chunk.map((t) => fetchChannelMessages(t)));
    const results = res.filter((r) => r.status === 'fulfilled').map((t) => t.value);
    const errors = res.filter((t) => t.status === 'rejected').map((t) => t.reason);
    console.log('Tg crawler completed with the following errors', errors);
    if (results.length < 1) continue;
    results.forEach((r) => {
      fs.writeFileSync(`${r.channel}.json`, JSON.stringify(r.result, null, 2), 'utf-8');
    });
  }
}
