import Redis from 'ioredis';
import { config } from 'dotenv';
config();

const redisClient = new Redis({
    port: process.env.REDIS_PORT ?? 6379,
    host: process.env.REDIS_HOST ?? 'localhost',
    retryStrategy: (times) => {
        const delay = Math.min(times * 50, 2000);
        return delay;
    },
    maxRetriesPerRequest: null
});

redisClient.on('connect', () => {
    console.log('Connected to Redis');
});

redisClient.on('error', (error) => {
    console.log(`Redis error: ${error}`);
});

export default redisClient;