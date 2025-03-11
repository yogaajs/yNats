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
        this._log('info', ...args);
    }

    public warn(...args: any[]): void {
        this._log('warn', ...args);
    }

    public error(...args: any[]): void {
        this._log('error', ...args);
    }

    // Private

    private _log(level: 'info' | 'warn' | 'error', ...args: any[]): void {
        console[level](`${this._prefix}`, ...args);
    }
}
