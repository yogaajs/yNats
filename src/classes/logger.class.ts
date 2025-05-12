/**
 * Simple logging utility class that prefixes all log messages
 * Provides standard logging methods with consistent formatting
 */
export default class Logger {
    /** Prefix string to prepend to all log messages */
    private readonly _prefix: string;
    
    /** Flag to control whether debug messages are displayed */
    private readonly _debug: boolean;
    
    /** Minimum time between alerts (default: 10_000) */
    private readonly _alertThresholdMs: number;

    /** Last alert timestamp for each message */
    private _alerts: Record<string, number> = {};

    /**
     * Creates a new logger instance
     * @param prefix Text to prepend to all log messages
     * @param debug Whether to enable debug logging (default: false)
     * @param alertThresholdMs Minimum time between alerts (default: 10_000)
     */
    constructor({
        prefix,
        debug,
        alertThresholdMs,
    }: {
        prefix: string;
        debug?: boolean;
        alertThresholdMs?: number;
    }) {
        if (!prefix) {
            throw new Error('Prefix is required!');
        }
        this._prefix = prefix;
        this._debug = debug ?? false;
        this._alertThresholdMs = alertThresholdMs ?? 10_000;
    }

    // Public methods

    /**
     * Logs informational messages
     * @param args Any values to log
     */
    public info(...args: any[]): void {
        console.info(this._prefix, ...args);
    }

    /**
     * Logs warning messages
     * @param args Any values to log
     */
    public warn(...args: any[]): void {
        console.warn(this._prefix, ...args);
    }

    /**
     * Logs error messages
     * @param args Any values to log
     */
    public error(...args: any[]): void {
        console.error(this._prefix, ...args);
    }

    /**
     * Logs debug messages if debug mode is enabled
     * @param args Any values to log
     */
    public debug(...args: any[]): void {
        if (this._debug) {
            console.debug(this._prefix, ...args);
        }
    }

    /**
     * Logs alert messages
     * @param id Unique identifier for the alert
     * @param args Any values to log
     */
    public alert(id: string, ...args: any[]): void {
        if (this._alerts[id] && Date.now() - this._alerts[id] < this._alertThresholdMs) {
            return;
        }

        this._alerts[id] = Date.now();
        console.warn(this._prefix, ...args);
    }
}
