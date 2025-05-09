/**
 * Simple logging utility class that prefixes all log messages
 * Provides standard logging methods with consistent formatting
 */
export default class Logger {
    /** Prefix string to prepend to all log messages */
    private readonly _prefix: string;
    
    /** Flag to control whether debug messages are displayed */
    private readonly _debug: boolean;
    
    /** Last alert timestamp for each message */
    private _lastAlert: Record<string, number> = {};

    /**
     * Creates a new logger instance
     * @param prefix Text to prepend to all log messages
     * @param debug Whether to enable debug logging (default: false)
     */
    constructor(prefix: string, debug: boolean = false) {
        this._prefix = prefix;
        this._debug = debug;
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
     * Logs error messages
     * @param args Any values to log
     */
    public alert(id: string, ms: number, ...args: any[]): void {
        if (this._lastAlert[id] && Date.now() - this._lastAlert[id] < ms) {
            return;
        }

        this._lastAlert[id] = Date.now();
        console.warn(this._prefix, ...args);
    }
}
