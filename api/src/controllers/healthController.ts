import { Request, Response } from 'express';
import { checkDbHealth } from '../db';

export async function getHealth(_req: Request, res: Response): Promise<void> {
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
}
