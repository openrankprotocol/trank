import { StringSession } from 'telegram/sessions';
import { getEnvs } from './envs';
import { TelegramClient } from 'telegram';

export const AppTgConfig = {
  timeWindowDays: 3,
  maxMessagesPerChannel: 2_000,
  parallelRequests: 3,
  rateLimitingDelay: 500,
  channels: ['thechaoschain', 'agent0kitchen', -1001772588611, 'ERC8004', -1003198190559],
};

export let TgClient: TelegramClient;

export const connectToTelegram = async () => {
  const envs = getEnvs();
  const session = new StringSession(envs.telegramAppSession);

  TgClient = new TelegramClient(session, envs.telegramAppId, envs.telegramAppHash, {
    connectionRetries: 5,
  });

  console.log('Connecting to telegram ...');
  await TgClient.connect();

  if (!(await TgClient.isUserAuthorized())) {
    throw new Error('Session expired. Please run the login script again.');
  }
  console.log('Connected to telegram!');
};
