import path from 'path';
import dotenv from 'dotenv';

// Load .env from root directory if present, otherwise fallback to local .env
dotenv.config({ path: path.resolve(__dirname, '../../.env') });
dotenv.config();

export interface AppConfig {
  postgres: {
    host: string;
    port: number;
    user: string;
    password?: string;
    database: string;
    ssl: boolean;
  };
  api: {
    port: number;
  };
  logLevel: string;
}

export function loadConfig(): AppConfig {
  const port = parseInt(process.env.API_PORT || '3000', 10);
  const pgPort = parseInt(process.env.POSTGRES_PORT || '5432', 10);

  return {
    postgres: {
      host: process.env.POSTGRES_HOST || 'localhost',
      port: Number.isNaN(pgPort) ? 5432 : pgPort,
      user: process.env.POSTGRES_USER || 'scheduler',
      password: process.env.POSTGRES_PASSWORD || '',
      database: process.env.POSTGRES_DB || 'scheduler',
      ssl: process.env.POSTGRES_SSLMODE === 'require',
    },
    api: {
      port: Number.isNaN(port) ? 3000 : port,
    },
    logLevel: process.env.LOG_LEVEL || 'info',
  };
}

export const config = loadConfig();
