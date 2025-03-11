import type { ConnectionOptions, NatsConnection, JetStreamClient, JetStreamManager } from "nats";
import { connect, DebugEvents, Events } from "nats";
import { options } from 'src/constants/instances';

import { Logger } from 'src/classes/Logger.class';
import { Readiness } from 'src/classes/Readiness.class';
import { StaticShutdownManager } from 'src/classes/Shutdown.class';


// Class
// ===========================================================

export class Client {
    private static readonly RETRY_TIMEOUT: number = 1000;
    private static readonly OPTIONS: ConnectionOptions = options;
    private static isStarting: boolean = false;

    private static readonly _logger: Logger = new Logger(`[nats][client]`);
    private static readonly _readiness: Readiness = new Readiness();

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

    // protected

    protected get nc(): NatsConnection {
        if (!Client._nc) {
            throw new Error('NATS connection not available');
        }
        return Client._nc;
    }

    protected get jsc(): JetStreamClient {
        if (!Client._jsc) {
            throw new Error('JetStream client not available');
        }
        return Client._jsc;
    }

    protected get jsm(): JetStreamManager {
        if (!Client._jsm) {
            throw new Error('JetStream manager not available');
        }
        return Client._jsm;
    }

    protected get clientReady(): Readiness {
        return Client._readiness;
    }

    // Private (local)

    private static async _connect(): Promise<void> {
        await Client._connectNatsServer();
        await Client._connectJetstreamClient();
        await Client._connectJetstreamManager();
    
        // Mark the client as ready so that waiting operations can proceed.
        Client._readiness.setReady();
    }

    private static async _disconnect(): Promise<void> {
        if (!Client._nc) {
            Client._logger.warn("No active connection to disconnect.");
            return;
        }
        try {
            Client._readiness.setShuttingDown(); // Avoid functions to be called while disconnecting
            await Client._nc.close();
            Client._logger.info("Disconnected from NATS server.");

        } catch (error) {
            Client._logger.error(`Failed to disconnect from server: ${(error as Error).message}`);

        } finally {
            Client._nc = null;
            Client._jsc = null;
            Client._jsm = null;
        }
    }

    private static async _connectNatsServer(attempt: number = 0): Promise<void> {
        if (Client._nc) {
            Client._logger.warn("Already connected to NATS server.");
            return;
        }
        while (true) {
            try {
                Client._nc = await connect(Client.OPTIONS);
                Client._logger.info("Connected to NATS server.");
                StaticShutdownManager.register(() => Client._disconnect());
                Client._listenServerEvents();
                break;

            } catch (error) {
                attempt++;
                Client._logger.error(
                    `Failed to connect to NATS server (attempt ${attempt}):`, 
                    (error as Error).message
                );
            }
            const delay = Math.min(Client.RETRY_TIMEOUT * Math.pow(2, attempt), 10_000); // 10s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    private static async _connectJetstreamClient(attempt: number = 0): Promise<void> {
        if (Client._jsc) {
            Client._logger.warn("Already connected to jetstream Client.");
            return;
        }
        while (true) {
            try {
                Client._jsc = Client._nc!.jetstream();
                Client._logger.info(`Connected to jetstream client`);
                break;

            } catch (error) {
                attempt++;
                Client._logger.error(
                    `Failed to connect to jetstream client (attempt ${attempt}):`, 
                    (error as Error).message
                );
            }
            const delay = Math.min(Client.RETRY_TIMEOUT * Math.pow(2, attempt), 10_000); // 10s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    private static async _connectJetstreamManager(attempt: number = 0): Promise<void> {
        if (Client._jsm) {
            Client._logger.warn("Already connected to jetstream manager.");
            return;
        }
        while (true) {
            try {
                Client._jsm = await Client._nc!.jetstreamManager();
                Client._logger.info(`Connected to jetstream manager`);
                break;

            } catch (error) {
                attempt++;
                Client._logger.error(
                    `Failed to connect to jetstream manager (attempt ${attempt}):`, 
                    (error as Error).message
                );
            }
            const delay = Math.min(Client.RETRY_TIMEOUT * Math.pow(2, attempt), 10_000); // 10s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    private static async _listenServerEvents(): Promise<void> {

        // Listen to connection events
        Client._nc!.closed().then((err: void | Error) => {
            if (err) {
                Client._logger.error(`NATS connection closed with an error:`, err.message);
            } else {
                Client._logger.info(`NATS connection closed.`);
            }
        });

        // Listen to connection events
        for await (const s of Client._nc!.status()) {
            switch (s.type) {
                // Client disconnected
                case Events.Disconnect:
                    Client._readiness.setNotReady();
                    Client._logger.info(`client disconnected - ${s.data}`);
                    break;
                // Client reconnected
                case Events.Reconnect:
                    Client._readiness.setReady();
                    Client._logger.info(`client reconnected - ${s.data}`);
                    break;
                // Client received a signal telling it that the server is transitioning to Lame Duck Mode
                case Events.LDM:
                    Client._logger.info("client has been requested to reconnect");
                    break;
                // Client received a cluster update
                case Events.Update:
                    Client._logger.info(`client received a cluster update - ${s.data}`);
                    break;
                // Client received an async error from the server
                case Events.Error:
                    Client._logger.info("client got a permissions error");
                    break;
                // Client is attempting to reconnect
                case DebugEvents.Reconnecting:
                    Client._readiness.setNotReady();
                    Client._logger.info("client is attempting to reconnect");
                    break;
                // Client has a stale connection
                case DebugEvents.StaleConnection:
                    Client._logger.info("client has a stale connection");
                    break;
                // Client initiated a reconnect
                case DebugEvents.ClientInitiatedReconnect:
                    Client._logger.info("client initiated a reconnect");
                    break;
                // Client ping timer
                case DebugEvents.PingTimer:
                    //Client.logger.info("client ping timer");
                    break;
                default:
                    Client._logger.error(`got an unknown status (${s.type}): ${JSON.stringify(s)}`);
            }
        }
    }
}
