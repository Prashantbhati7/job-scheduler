import { app } from './app';
import { config } from './config';
import { closeDb,checkDbHealth} from './db';

const server = app.listen(config.api.port, () => {
  console.log(
    JSON.stringify({
      level: 'info',
      message: `Job Scheduler API started on port ${config.api.port}`,
      port: config.api.port,
    })
  );
});

async function shutdown(signal: string): Promise<void> {
  console.log(JSON.stringify({ level: 'info', message: `Received ${signal}, shutting down gracefully...` }));

  server.close(async () => {
    console.log(JSON.stringify({ level: 'info', message: 'HTTP server closed.' }));
    try {
      await closeDb();
      console.log(JSON.stringify({ level: 'info', message: 'PostgreSQL connection pool drained.' }));
      process.exit(0);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(JSON.stringify({ level: 'error', message: 'Error closing PostgreSQL pool', error: message }));
      process.exit(1);
    }
  });

  // Force close after 5 seconds if graceful shutdown hangs
  setTimeout(() => {
    console.error(JSON.stringify({ level: 'error', message: 'Shutdown timed out, forcing exit' }));
    process.exit(1);
  }, 5000).unref();
}

process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});
