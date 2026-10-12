import express, { Application, Request, Response, NextFunction } from 'express';
import { healthRouter } from './routes/health';

export const app: Application = express();

app.use(express.json());
// logger 
app.use((req: Request, res: Response, next: NextFunction) => {
  const start = Date.now();
  res.on('finish', () => {
    const durationMs = Date.now() - start;
    console.log(
      JSON.stringify({
        level: 'info',
        method: req.method,
        path: req.originalUrl,
        status: res.statusCode,
        durationMs,
      })
    );
  });
  next();
});

app.use(healthRouter);

// 404 handler
app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: 'Not Found' });
});

// Global error handler
app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  console.error(JSON.stringify({ level: 'error', message: 'Unhandled server error', error: err.message }));
  res.status(500).json({ error: 'Internal Server Error' });
});
