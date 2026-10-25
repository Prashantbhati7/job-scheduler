import express, { Application, Request, Response, NextFunction } from 'express';
import { healthRouter } from './routes/health';
import { jobsRouter } from './routes/jobs';

export const app: Application = express();

app.use(express.json());

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
app.use('/jobs', jobsRouter);

app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: 'Not Found' });
});

app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
  const status = typeof err.statusCode === 'number' ? err.statusCode : 500;
  if (status >= 500) {
    console.error(
      JSON.stringify({
        level: 'error',
        message: 'Unhandled server error',
        error: err.message,
        stack: err.stack,
      })
    );
    res.status(500).json({ error: 'Internal Server Error' });
    return;
  }
  res.status(status).json({ error: err.message });
});
