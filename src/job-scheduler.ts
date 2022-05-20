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
        this.runExecute();
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
                //添加到任务队列中
                await this.redisClient.rpush('execute:', newTask);
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
                    //放入执行队列中执行
                    await this.redisClient.rpush('execute:', executedTask);
                }
            }
        }, 1)
    }

    /**
     * 执行任务队列中需要跑的
     */
    public async runExecute() {
        while (true) {
            console.info("存在可以执行的任务")
            const task = await this.redisClient.blpop('execute:' , 3000);
            //[ 'execute:', '["2e8aa6c5-8343-4bcb-b8b3-73a322d4120d",["1"]]' ]
            if (!task) continue;
            const executedTaskJson = JSON.parse(task[1]);
            this.runTask(executedTaskJson[1], executedTaskJson);
        }
    }

    public async runTask(args: [string], executedTask: string) {
        try {
            await this.callback(args);
        } catch (err) {
            console.log('[JobScheduler] This task fails and is added to the failure queue');
        }
    }
}