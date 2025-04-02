import { ConnectionOptions, NatsConnection, JetStreamClient, JetStreamManager } from "./types";
import { connect } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { instances } from 'src/constants/instances';

import { Logger } from 'src/classes/Logger.class';
import { Operator } from '@/classes/Operator.class';
import { Readiness } from 'src/classes/Readiness.class';
import { StaticShutdownManager } from 'src/classes/Shutdown.class';

// Class
// ===========================================================

export class Client {
    private static readonly RETRY_TIMEOUT: number = 1_000;
    private static readonly CLIENT_OPTIONS: ConnectionOptions = instances;

    private static readonly _logger: Logger = new Logger(`[nats][client]`);
    private static readonly _readiness: Readiness = new Readiness();
    private static readonly _operator: Operator = new Operator();

    private static isStarting: boolean = false;
    private static _nc: NatsConnection | null = null;
    private static _jsc: JetStreamClient | null = null;
    private static _jsm: JetStreamManager | null = null;

    // Constructor

    protected constructor() {
        if (Client.isStarting !== true) {
            Client.isStarting = true;
            Client._connect();
        }
    }

    // Protected

    protected get natsConnection(): NatsConnection {
        if (!Client._nc) {
            throw new Error('NATS connection not available');
        }
        return Client._nc;
    }

    protected get jetstreamClient(): JetStreamClient {
        if (!Client._jsc) {
            throw new Error('JetStream client not available');
        }
        return Client._jsc;
    }

    protected get jetstreamManager(): JetStreamManager {
        if (!Client._jsm) {
            throw new Error('JetStream manager not available');
        }
        return Client._jsm;
    }

    protected get clientReady(): Readiness {
        return Client._readiness;
    }

    protected get clientOperator(): Operator {
        return Client._operator;
    }

    // Private (local)

    private static async _connect(): Promise<void> {
        await Client._connectNatsServer();
        await Client._connectJetstreamClient();
        await Client._connectJetstreamManager();

        // Listen to server events
        Client._listenServerEvents();
    
        // Register a shutdown handler
        StaticShutdownManager.register(() => Client._disconnect());

        // Mark the client as ready so that waiting operations can proceed.
        Client._readiness.setReady();
    }

    private static async _disconnect(): Promise<void> {
        try {
            // Wait for all operations to finish
            await this._operator.waitForAll(
                (name, count) => 
                    Client._logger.info(`Waiting for ${count} ${name} operations to finish...`)
            );

            // Stop new operations
            Client._readiness.setNotReady();

            // Drain the connection and close it
            Client._logger.info("Draining connection...");
            await Client._nc!.drain();

        } catch (error) {
            Client._logger.error(`Failed to disconnect from server: ${(error as Error).message}`);
        }
    }

    // Private (local)

    private static async _connectNatsServer(attempt: number = 0): Promise<void> {
        if (this._nc) {
            this._logger.warn("Already connected to NATS server.");
            return;
        }
        while (true) {
            try {
                this._nc = await connect(Client.CLIENT_OPTIONS);
                this._logger.info("Connected to NATS server.");
                break;

            } catch (error) {
                attempt++;
                this._logger.error(
                    `Failed to connect to NATS server (attempt ${attempt}):`, 
                    (error as Error).message
                );
            }
            const delay = Math.min(Client.RETRY_TIMEOUT * Math.pow(2, attempt), 5_000); // 5s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    private static async _connectJetstreamClient(attempt: number = 0): Promise<void> {
        if (this._jsc) {
            this._logger.warn("Already connected to jetstream Client.");
            return;
        }
        while (true) {
            try {
                this._jsc = jetstream(this._nc!);
                this._logger.info(`Connected to jetstream client`);
                break;

            } catch (error) {
                attempt++;
                this._logger.error(
                    `Failed to connect to jetstream client (attempt ${attempt}):`, 
                    (error as Error).message
                );
            }
            const delay = Math.min(Client.RETRY_TIMEOUT * Math.pow(2, attempt), 5_000); // 5s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    private static async _connectJetstreamManager(attempt: number = 0): Promise<void> {
        if (this._jsm) {
            this._logger.warn("Already connected to jetstream manager.");
            return;
        }
        while (true) {
            try {
                this._jsm = await jetstreamManager(this._nc!);
                this._logger.info(`Connected to jetstream manager`);
                break;

            } catch (error) {
                attempt++;
                this._logger.error(
                    `Failed to connect to jetstream manager (attempt ${attempt}):`, 
                    (error as Error).message
                );
            }
            const delay = Math.min(Client.RETRY_TIMEOUT * Math.pow(2, attempt), 5_000); // 5s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    private static async _listenServerEvents(): Promise<void> {

        // Listen to connection events
        this._nc?.closed().then((err: void | Error) => {
            this._readiness.setNotReady();
            if (err) {
                this._logger.error(`NATS connection closed with an error:`, err.message);
                // Potential reconnect
            } else {
                this._logger.info(`NATS connection closed.`);
            }
        });

        // Listen to connection events
        for await (const s of this._nc!.status()) {
            switch (s.type) {
                // Client disconnected
                case "disconnect":
                    this._readiness.setNotReady();
                    this._logger.info(`client disconnected - ${s.server}`);
                    break;
                // Client reconnected
                case "reconnect":
                    this._readiness.setReady();
                    this._logger.info(`client reconnected - ${s.server}`);
                    break;
                // Client is attempting to reconnect
                case "reconnecting":
                    this._readiness.setNotReady();
                    this._logger.info("client is attempting to reconnect...");
                    break;
                // Client received a signal telling it that the server is transitioning to Lame Duck Mode
                case "ldm":
                    this._logger.info(`client transitioning to Lame Duck Mode - ${s.server}`);
                    break;
                // Client received a cluster update
                case "update":
                    if (s.added && s.added?.length > 0) {
                        this._logger.info(`cluster update - ${s.added} added`);
                    }
                    if (s.deleted && s.deleted?.length > 0) {
                        this._logger.info(`cluster update - ${s.deleted} removed`);
                    }
                    break;
                // Client received an async error from the server
                case "error":
                    this._logger.info(`client got a permissions error - ${s.error}`);
                    break;
                // Client has a stale connection
                case "staleConnection":
                    this._logger.info("client has a stale connection");
                    break;
                // Client initiated a reconnect
                case "forceReconnect":
                    this._logger.info("client initiated a reconnect");
                    break;
                // Client is slow
                case "slowConsumer":
                    this._logger.info(`client is slow - ${s.sub.getSubject()} ${s.pending} pending messages`);
                    break;
                // Unknown status
                default:
                    this._logger.error(`got an unknown status (${s.type}): ${JSON.stringify(s)}`);
            }
        }
    }
}
