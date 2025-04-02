import type { Msg, NatsError, Consumer, Subscription } from "nats";
import { Client } from 'src/client/Client';
import { Logger } from '@/classes/Logger.class';
import { Operator } from '@/classes/Operator.class';

// Class
// ===========================================================

export type ApiHandler = (msg: Msg) => void | Promise<void>;

export class Responder extends Client {
    private readonly _logger: Logger = new Logger(`[nats][responder]`);

    private _subscriptions = new Set<Subscription>();
    private _subscriptionsCount = 1;
    private _subscriptionsInitialized = false;


    private handlers = new Map<string, ApiHandler>();
    private stacks = new Map<string, any[]>();
    private running = new Set<string>();

    // Constructor

    constructor({ subscriptionCount = 1 }: { subscriptionCount?: number }) {
        super();

        this._subscriptionsCount = subscriptionCount;
    }

    // Public

    public async register(endpoint: string, handler: ApiHandler) {
        const subject = `api.${endpoint}`;

        // Initialize subscriptions if not already done
        if (this._subscriptionsInitialized !== true) {
            this._subscriptionsInitialized = true;
            await this._startSubscriptions();
        }

        // Check if handler is already registered
        if (this.handlers.has(subject)) {
            throw new Error(`Handler already registered for subject ${subject}`);
        }

        // Register handler
        this.handlers.set(subject, handler);
    }

    // Private

    private async _startSubscriptions(): Promise<void> {
        await this.clientReady.isReady();

        const processData = async (err: NatsError | null, msg: Msg) => {
            if (err) {
                this._logger.error(`Error on ${msg.subject}:`, err);
                return;
            }
            const stack = this.stacks.get(msg.subject);
            if (stack) {
                stack.push(msg);
            } else {
                this.stacks.set(msg.subject, [msg]);
            }
            this._processMessage(msg);
        };

        // Subscribe to all subjects that start with "api."
        for (let i = 0; i < this._subscriptionsCount; i++) {
            const subscription = this.nc.subscribe("api.>", {
                queue: "api-processors",
                callback: processData
            });
            this._subscriptions.add(subscription);
            this._logger.info(`Subscribtor ${i} started on ${subscription.getSubject()}`);
        }
    }

    private async _processMessage(msg: Msg): Promise<void> {
        if (this.running.has(msg.subject)) {
            return;
        }

        this.running.add(msg.subject);

        const handler = this.handlers.get(msg.subject);
        if (!handler) {
            throw new Error(`Handler not registered for subject ${msg.subject}`);
        }

        const stack = this.stacks.get(msg.subject);
        if (!stack) {
            throw new Error(`Stack not found for subject ${msg.subject}`);
        }

        while (stack.length > 0) {
            const msg = stack.shift()!;
            try {
                await handler(msg);
            } catch (error) {
                console.error(`Error processing message on subject ${msg.subject}:`, error);
            }
        }

        this.running.delete(msg.subject);
    }
}
