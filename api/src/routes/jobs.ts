import { Router } from 'express';
import * as jobController from '../controllers/jobController';

export const jobsRouter = Router();

jobsRouter.post('/', jobController.createJob);
jobsRouter.get('/', jobController.listJobs);
jobsRouter.get('/:id', jobController.getJobById);
jobsRouter.patch('/:id/pause', jobController.pauseJob);
jobsRouter.patch('/:id/resume', jobController.resumeJob);
jobsRouter.delete('/:id', jobController.deleteJob);
