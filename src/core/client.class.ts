import type { ConnectionOptions, NatsConnection, JetStreamClient, JetStreamManager } from "./types";
import { connect } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";

import { Streams } from "./streams.class";
import { Consumers } from "./consumers.class";

import Logger from 'src/classes/logger.class';
import Manager from 'src/classes/manager.class';
import Operator from 'src/classes/operator.class';

// Constants
// ===========================================================

const RETRY_TIMEOUT = 1_000;

const logger = new Logger({ prefix: "[nats][client]" });
const manager = new Manager();
const operator = new Operator();

// Class
// ===========================================================

export class Client {
    private isInitializing: boolean = false;
    private connectionOptions: ConnectionOptions | null = null;

    private nc: NatsConnection | null = null;
    private js: JetStreamClient | null = null;
    private jsm: JetStreamManager | null = null;

    private _stream: Streams;
    private _consumer: Consumers;

    // Constructor

    constructor(connectionOptions: ConnectionOptions) {
        this.connectionOptions = connectionOptions;
        this.initialize();

        this._stream = new Streams(this);
        this._consumer = new Consumers(this);
    }

    // Protected

    public get stream(): Streams {
        if (!this._stream) {
            throw new Error('Stream not available');
        }
        return this._stream;
    }

    public get consumer(): Consumers {
        if (!this._consumer) {
            throw new Error('Consumer not available');
        }
        return this._consumer;
    }

    public get natsConnection(): NatsConnection {
        if (!this.nc) {
            throw new Error('NATS connection not available');
        }
        return this.nc;
    }

    public get jetstreamClient(): JetStreamClient {
        if (!this.js) {
            throw new Error('JetStream client not available');
        }
        return this.js;
    }

    public get jetstreamManager(): JetStreamManager {
        if (!this.jsm) {
            throw new Error('JetStream manager not available');
        }
        return this.jsm;
    }

    public get clientReady(): Manager {
        return manager;
    }

    public get clientManager(): Manager {
        return manager;
    }

    public get clientCleaner(): Operator {
        return operator;
    }

    // Public

    public async isReady(): Promise<void> {
        return manager.isReady();
    }

    public async shutdown(): Promise<void> {
        logger.info("Shutting down client...");

        // Wait for all operations to finish
        await operator.cleanAll(
            (remaining) => {
                logger.info(`Waiting for ${remaining} operations to finish...`);
            }
        );

        // Drain the connection and close it
        logger.info("Draining connection...");
        await this.nc!.drain();
    }

    // Private

    private async initialize(): Promise<void> {
        if (this.nc) {
            logger.warn("Client is already initialized!");
            return;
        }
        if (this.isInitializing === true) {
            logger.warn("Client is already initializing!");
            return;
        }

        this.isInitializing = true;

        // Initialize the NATS connection
        await this.initNatsConnection()

        // Initialize all the components
        await this.listenServerEvents();
        await this.initJetstreamClient();
        await this.initJetstreamManager();

        // Set the client as ready
        setTimeout(() => {
            manager.setReady();
            this.isInitializing = false;
        }, 200);
    }

    private async initNatsConnection(attempt: number = 0): Promise<void> {
        if (this.nc) {
            logger.warn(`Connection to NATS server already initialized!`);
            return;
        }
        while (true) {
            try {
                this.nc = await connect(this.connectionOptions!);
                logger.info(`Connection to NATS server initialized.`);
                logger.info(`Max payload:`, this.nc!.info!.max_payload);
                break;

            } catch (error) {
                attempt++;
                logger.error(`Failed to initialize connection to NATS server (attempt ${attempt}):`, 
                    (error as Error).message
                );
            }
            // Wait for the next attempt (retry)
            const delay = Math.min(RETRY_TIMEOUT * attempt, 15_000); // 15s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    private async initJetstreamClient(attempt: number = 0): Promise<void> {
        if (this.js) {
            logger.warn(`Jetstream client is already initialized!`);
            return;
        }
        while (true) {
            if (!this.nc) {
                logger.error(`NATS connection not initialized!`);
                break;
            }
            try {
                this.js = jetstream(this.nc);
                logger.info(`Jetstream client initialized.`);
                break;

            } catch (error) {
                attempt++;
                logger.error(`Failed to initialize jetstream client (attempt ${attempt}):`, 
                    (error as Error).message
                );
            }
            // Wait for the next attempt (retry)
            const delay = Math.min(RETRY_TIMEOUT * attempt, 15_000); // 15s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    private async initJetstreamManager(attempt: number = 0): Promise<void> {
        if (this.jsm) {
            logger.warn("Jetstream manager is already initialized!");
            return;
        }
        while (true) {
            if (!this.nc) {
                logger.error(`NATS connection not initialized!`);
                break;
            }
            try {
                this.jsm = await jetstreamManager(this.nc);
                logger.info(`Jetstream manager initialized.`);
                break;

            } catch (error) {
                attempt++;
                logger.error( `Failed to initialize jetstream manager (attempt ${attempt}):`, 
                    (error as Error).message
                );
            }
            // Wait for the next attempt (retry)
            const delay = Math.min(RETRY_TIMEOUT * attempt, 15_000); // 15s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    private async listenServerEvents(): Promise<void> {
        const natsConnection = this.nc!;

        // Listen to connection events
        natsConnection.closed().then((err: void | Error) => {
            manager.setNotReady();
            if (err) {
                logger.error(`NATS connection closed with an error:`, err.message, err.cause);
                manager.setNotReady();
            } else {
                logger.info(`NATS connection closed.`);
            }
        });

        (async () => {
            // Listen to connection events
            for await (const s of natsConnection.status()) {
                switch (s.type) {
                    // Client disconnected
                    case "disconnect":
                        manager.setNotReady();
                        logger.info(`client disconnected - ${s.server}`);
                        break;
                    // Client is attempting to reconnect
                    case "reconnecting":
                        manager.setNotReady();
                        logger.info("client is attempting to reconnect...");
                        break;
                    // Client reconnected
                    case "reconnect":
                        manager.setReady();
                        logger.info(`client reconnected - ${s.server}`);
                        break;
                    // Client received a signal telling it that the server is transitioning to Lame Duck Mode
                    case "ldm":
                        logger.info(`client transitioning to Lame Duck Mode - ${s.server}`);
                        break;
                    // Client received a cluster update
                    case "update":
                        if (s.added && s.added?.length > 0) {
                            logger.info(`cluster update - ${s.added} added`);
                        }
                        if (s.deleted && s.deleted?.length > 0) {
                            logger.info(`cluster update - ${s.deleted} removed`);
                        }
                        break;
                    // Client received an async error from the server
                    case "error":
                        logger.info(`client got an error - ${s.error}`);
                        break;
                    // Client has a stale connection
                    case "staleConnection":
                        logger.info("client has a stale connection");
                        break;
                    // Client initiated a reconnect
                    case "forceReconnect":
                        logger.info("client initiated a reconnect");
                        break;
                    // Client is slow
                    case "slowConsumer":
                        logger.info(`client is slow - ${s.sub.getSubject()} ${s.pending} pending messages`);
                        break;
                    // Ping
                    case "ping":
                        break;
                    // Unknown status
                    default:
                        // @ts-ignore
                        logger.error(`got an unknown status (${s.type}): ${JSON.stringify(s)}`);
                }
            }
        })().then();
    }
}
