import { Request, Response, NextFunction } from 'express';
import * as jobService from '../services/jobService';

function getParamId(req: Request): string {
  const { id } = req.params;
  return Array.isArray(id) ? (id[0] || '') : (id || '');
}

export async function createJob(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const job = await jobService.createJob(req.body);
    res.status(201).json(job);
  } catch (err) {
    next(err);
  }
}

export async function listJobs(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const limit = req.query.limit ? parseInt(req.query.limit as string, 10) : undefined;
    const offset = req.query.offset ? parseInt(req.query.offset as string, 10) : undefined;
    const enabled = req.query.enabled !== undefined ? req.query.enabled === 'true' : undefined;

    const result = await jobService.listJobs({ limit, offset, enabled });
    res.status(200).json(result);
  } catch (err) {
    next(err);
  }
}

export async function getJobById(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const job = await jobService.getJobById(getParamId(req));
    res.status(200).json(job);
  } catch (err) {
    next(err);
  }
}

export async function pauseJob(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const job = await jobService.pauseJob(getParamId(req));
    res.status(200).json({ message: 'job paused', job });
  } catch (err) {
    next(err);
  }
}

export async function resumeJob(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const job = await jobService.resumeJob(getParamId(req));
    res.status(200).json({ message: 'job resumed', job });
  } catch (err) {
    next(err);
  }
}

export async function deleteJob(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const result = await jobService.deleteJob(getParamId(req));
    res.status(200).json({ message: 'job deleted', id: result.id });
  } catch (err) {
    next(err);
  }
}
