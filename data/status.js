class Status {
    static #status =  'Idle';
    static #job = 'None'
    static #updates = [];
    static #startTime;
    static #completeTime;

    static getStatus() {
        return this.#status;
    }

    static updateStatus(status) {
        this.#status = status;
        if(status === 'Idle'){
            this.#completeTime = Date.now();
        }
    }

    static getJob() {
        return this.#job;
    }

    static newJob(job) {
        if(this.#status === 'Idle'){
            this.#job = job;
            this.#status = 'Started'
            this.#updates = [];
            this.#startTime = Date.now();
            return true;
        } else {
            return false;
        }
    }

    static getUpdates(skip =0 ){
        return this.#updates.slice(skip);
    }

    static pushUpdate(update){
        this.#updates.push(update)
    }

    static delay(time) {
        return new Promise(resolve => setTimeout(resolve, time));
    }

}

export default Status;