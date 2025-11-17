import path from 'path';
require('dotenv').config({ path: path.resolve(__dirname, '../.env') });

import readline from 'readline';
import { StringSession } from 'telegram/sessions';
import { getEnvs } from './config/envs';
import { TelegramClient } from 'telegram';

const stringSession = new StringSession('');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function start() {
  console.log('Loading interactive example...');
  const envs = getEnvs();
  const client = new TelegramClient(stringSession, envs.telegramAppId, envs.telegramAppHash, {
    connectionRetries: 5,
  });
  await client.start({
    phoneNumber: async () =>
      new Promise((resolve) => rl.question('Please enter your number: ', resolve)),
    password: async () =>
      new Promise((resolve) => rl.question('Please enter your password: ', resolve)),
    phoneCode: async () =>
      new Promise((resolve) => rl.question('Please enter the code you received: ', resolve)),
    onError: (err) => console.log(err),
  });
  console.log('You should now be connected. Copy your session below ⬇️');
  console.log(client.session.save());
}

start().catch((e) => {
  console.error(e);
  process.exit(1);
});
