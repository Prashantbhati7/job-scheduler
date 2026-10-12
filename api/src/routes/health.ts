import { Router, Request, Response } from 'express';
import { checkDbHealth } from '../db';

export const healthRouter = Router();

healthRouter.get('/health', async (_req: Request, res: Response) => {
  const isDbHealthy = await checkDbHealth();

  if (isDbHealthy) {
    res.status(200).json({
      status: 'ok',
      db: 'up',
    });
    return;
  }

  res.status(503).json({
    status: 'degraded',
    db: 'down',
  });
});
