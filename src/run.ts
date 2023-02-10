import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
    maxThreads = Math.max(0, maxThreads);

    return new Promise((resolve) => {

        type fnNext = (err: null, result: ITask) => void
        type fnPr = (task: ITask, fnNext: fnNext) => void

        class Queue {

            concurrency: number;
            count: number;
            waiting: ITask[][];
            onProcess: fnPr | null;
            factors: number[];

            constructor(concurrency:number) {
                this.concurrency = concurrency || Infinity;
                this.count = 0;
                this.waiting = [];
                this.onProcess = null;
                this.factors = [];
            }

            // factors change the state of the channel
            // 0 free 
            // 1 busy
            // 2 free and have
            // 3 busy and have
        
            static channels(concurrency:number) {
                return new Queue(concurrency);
            }
        
            add(task: ITask) {
                const hasChannel = this.count < this.concurrency;
                if (task) {
                    const targetId = task.targetId
                    if (hasChannel && !this.factors[targetId]) {
                        this.factors[targetId] = 1;
                        this.next(task);
                        return;
                    }
                    // if added targetId
                    if (this.waiting[targetId]) {
                        if (this.factors[targetId] === 1) {
                            this.factors[targetId] = 3;
                        } else {
                            this.factors[targetId] = 2;
                        }
                        this.waiting[targetId].push(task);
                    // not added yet
                    } else if (!this.waiting[targetId]) {
                        if (this.factors[targetId] === 1) {
                            this.factors[targetId] = 3;
                        } else {
                            this.factors[targetId] = 2;
                        }
                        this.waiting[targetId] = [task];
                    }
                }
            }
        
            next(task: ITask) {
                this.count++;
                if(this.onProcess) {
                    this.onProcess(task, (err, result) => {
                        const targetId = result.targetId;
                        // change channels state
                        if (this.factors[targetId] === 1) {
                            this.factors[targetId] = 0;
                        } else if (this.factors[targetId] === 3) {
                            this.factors[targetId] = 2;
                        }
                        if (this.waiting[targetId]) {
                            if (this.waiting[targetId].length === 0) {
                                this.factors[targetId] = 0;
                            }
                        }
                        this.count--;

                        if (this.waiting.length > 0) {
                            this.takeNext();
                        }
                        
                        if (this.count === 0) {
                            fnAdd(0);
                        }
                    });
                }
            }

            takeNext() {
                const id = this.factors.indexOf(2)
                if (id === -1) return;
                const task = this.waiting[id].shift();
                if (task) {
                    this.factors[id] = 3;
                    this.next(task)
                }
            }

            process(listener: fnPr) {
                this.onProcess = listener;
                return this;
            }
        }

        // Usage
        const job = (task: ITask, next: fnNext) => {
            executor.executeTask(task).then(() => {
                next(null, task);
            })
        };
        
        
        const queueSh = Queue.channels(maxThreads)
            .process(job);

        const fnAdd = async (count: number) => {
            const iterator = queue[Symbol.asyncIterator]();
            const task = await iterator.next()
            if (task.value && task.value.targetId < 20) {
                queueSh.add(task.value);
                fnAdd(1);
            } else if (count === 0) {
                resolve(true);
            }
            
        }
        fnAdd(0)
    })
}
