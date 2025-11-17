export const getEnvs = () => {
  return {
    telegramAppId: Number(process.env.TELEGRAM_APP_ID || '0'),
    telegramAppHash: process.env.TELEGRAM_APP_HASH || '',
    telegramAppSession: process.env.TELEGRAM_APP_SESSION || '',
  };
};
