import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { taskParams } from "./types/task.type";

export class JobScheduler {

    private redisClient = new Redis({
        port: 6379,
        host: "127.0.0.1",
        db: 0 //可以删除掉哦
    });
    private callback: (args: [string]) => void;

    constructor(callback: (args: [string]) => void) {
        this.callback = callback;
        this.watchDelayed();
        this.runExecuted();
    }

    public async addTask(task: taskParams): Promise<string> {
        const identifier = uuidv4();
        const newTask = JSON.stringify([identifier, task.args]);

        if (task.delay > 0) {
            try {
                this.redisClient.zadd('delayed:', new Date().getTime() + task.delay, newTask);
                console.log(`task ${task.args[0]} added to the delayed Q, it will start in ${task.delay} ms`);
            } catch (err) {
                console.log('[JobScheduler] failed to added to the delayed queue, retry?');
            }
        } else {
            try {
                this.runCallback(task.args, newTask)
            } catch (err) {
                console.log('[JobScheduler] This task fails and is added to the failure queue');
            }
        }
        return identifier;
    }

    public async watchDelayed() {
        setInterval(async () => {
            const delayedTask = await this.redisClient.zrange('delayed:', 0, 0, 'WITHSCORES');
            if (delayedTask && Number(delayedTask[1]) < new Date().getTime()) {
                const executedTask = delayedTask[0];
                if (await this.redisClient.zrem('delayed:', executedTask)) {
                    const executedTaskJson = JSON.parse(executedTask);
                    await this.redisClient.rpush('execute:', executedTask);
                    this.runCallback(executedTaskJson[1], executedTask);
                }
            }
        }, 1)
    }

    public async runExecuted() {
        while (await this.redisClient.llen('execute:')) {
            const task = await this.redisClient.lpop('execute:');
            if (task) {
                const executedTaskJson = JSON.parse(task);
                this.addTask({ args: executedTaskJson[1], delay: 0 });
            }

        }
    }

    public async runCallback(args: [string], executedTask: string) {
        await this.redisClient.rpush('execute:', executedTask);
        try {
            await this.callback(args);
        } catch (err) {
            console.log('[JobScheduler] This task fails and is added to the failure queue');
        }
        await this.redisClient.lrem('execute:', 1, executedTask);
    }
}