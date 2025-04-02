// Class
// ===========================================================

export class Logger {
    private readonly _prefix: string;

    // Constructor

    constructor(prefix: string) {
        this._prefix = prefix;
    }

    // Public

    public info(...args: any[]): void {
        console.info(`${this._prefix}`, ...args);
    }

    public warn(...args: any[]): void {
        console.warn(`${this._prefix}`, ...args);
    }

    public error(...args: any[]): void {
        console.error(`${this._prefix}`, ...args);
    }
}
