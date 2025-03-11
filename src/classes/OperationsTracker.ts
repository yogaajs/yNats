// Class
// ===========================================================

export class OperationsTracker {
    private activeOperations = new Set<Promise<any>>();

    // Private (local)

    public track<T>(operation: Promise<T>): Promise<T> {
        this.activeOperations.add(operation);
        
        return operation.finally(() => {
            this.activeOperations.delete(operation);
        });
    }

    public async waitForOperations(): Promise<void> {
        if (this.activeOperations.size === 0) return;

        let watchdog = false;
        setTimeout(() => watchdog = true, 30_000);

        while (this.activeOperations.size > 0) {
            await Promise.race(
                this.activeOperations.values()
            );
            console.log('OperationsTracker: waitForOperations: activeOperations.size', this.count);
            if (watchdog) {
                throw new Error('Shutdown timeout');
            }
        }
    }

    public get count(): number {
        return this.activeOperations.size;
    }
}
