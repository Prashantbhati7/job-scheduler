import { Router } from 'express';
import { getHealth } from '../controllers/healthController';

export const healthRouter = Router();

healthRouter.get('/health', getHealth);
