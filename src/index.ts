import { randomUUID } from 'node:crypto';
import express, { type Request, type Response } from 'express';
import { Kafka, type Consumer, type Producer } from 'kafkajs';
import type { CatalogAvailability, InventoryAdjusted, InventoryAdjustmentRequest } from './types';

const host = process.env.INVENTORY_SYNC_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.INVENTORY_SYNC_PORT || '9011', 10);
const kafkaBrokers = (process.env.INVENTORY_SYNC_KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const kafkaTopic = process.env.INVENTORY_ADJUSTMENT_TOPIC || 'inventory.adjustment.request';
const inventoryAdjustedTopic = process.env.INVENTORY_ADJUSTED_TOPIC || 'inventory.adjusted';
const catalogBaseUrl = process.env.CATALOG_BASE_URL || 'http://localhost:5112';

const app = express();
app.use(express.json({ limit: '1mb' }));

const kafka = new Kafka({
  clientId: 'inventory-sync-service',
  brokers: kafkaBrokers
});
const consumer: Consumer = kafka.consumer({ groupId: 'inventory-sync-service-group' });
const producer: Producer = kafka.producer();

const processedEvents: InventoryAdjusted[] = [];
const maxProcessedEvents = 100;
let kafkaConnected = false;
const allowedAdjustmentReasons = new Set([
  'ORDER_PLACED',
  'ORDER_CANCELLED',
  'RETURN_RECEIVED',
  'MANUAL_RECONCILIATION'
]);

function isInventoryAdjustmentRequest(value: unknown): value is InventoryAdjustmentRequest {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }

  const payload = value as Record<string, unknown>;
  return (
    typeof payload.requestId === 'string' &&
    typeof payload.sku === 'string' &&
    typeof payload.adjustmentDelta === 'number' &&
    typeof payload.reason === 'string' &&
    allowedAdjustmentReasons.has(payload.reason) &&
    typeof payload.requestedAt === 'string'
  );
}

function rememberEvent(event: InventoryAdjusted): void {
  processedEvents.push(event);
  if (processedEvents.length > maxProcessedEvents) {
    processedEvents.shift();
  }
}

function buildBootstrapAdjustedEvent(): InventoryAdjusted {
  return {
    eventId: randomUUID(),
    requestId: `bootstrap-${randomUUID()}`,
    sku: 'bootstrap-sku',
    quantityOnHand: 1,
    available: true,
    publishedAt: new Date().toISOString()
  };
}

function startHeartbeatPublisher(): void {
  setInterval(() => {
    const event = buildBootstrapAdjustedEvent();
    void publishAdjustedEvent(event)
      .then(() => rememberEvent(event))
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`heartbeat publish failed: ${message}`);
      });
  }, 3000);
}

async function fetchCatalogAvailability(sku: string): Promise<CatalogAvailability | null> {
  try {
    const response = await fetch(`${catalogBaseUrl}/catalog/items/${encodeURIComponent(sku)}/availability`);
    if (!response.ok) {
      return null;
    }

    const payload = (await response.json()) as unknown;
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      return null;
    }

    const typedPayload = payload as Record<string, unknown>;
    if (
      typeof typedPayload.sku !== 'string' ||
      typeof typedPayload.available !== 'boolean' ||
      typeof typedPayload.quantityOnHand !== 'number' ||
      typeof typedPayload.backorderable !== 'boolean'
    ) {
      return null;
    }

    return {
      sku: typedPayload.sku,
      available: typedPayload.available,
      quantityOnHand: typedPayload.quantityOnHand,
      backorderable: typedPayload.backorderable
    };
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`catalog lookup failed for sku=${sku}: ${message}`);
    return null;
  }
}

async function publishAdjustedEvent(event: InventoryAdjusted): Promise<void> {
  await producer.send({
    topic: inventoryAdjustedTopic,
    messages: [{ key: event.sku, value: JSON.stringify(event) }]
  });
}

async function processAdjustment(request: InventoryAdjustmentRequest): Promise<InventoryAdjusted> {
  const availability = await fetchCatalogAvailability(request.sku);
  const quantityOnHand = Math.max(availability?.quantityOnHand ?? 0, 0);
  const adjusted: InventoryAdjusted = {
    eventId: randomUUID(),
    requestId: request.requestId,
    sku: request.sku,
    quantityOnHand,
    available: availability?.available ?? quantityOnHand > 0,
    publishedAt: new Date().toISOString()
  };

  await publishAdjustedEvent(adjusted);
  rememberEvent(adjusted);
  return adjusted;
}

async function startKafkaConsumer(): Promise<void> {
  await consumer.connect();
  await producer.connect();
  kafkaConnected = true;
  await consumer.subscribe({ topic: kafkaTopic, fromBeginning: false });

  const bootstrapEvent = buildBootstrapAdjustedEvent();
  await publishAdjustedEvent(bootstrapEvent);
  rememberEvent(bootstrapEvent);
  startHeartbeatPublisher();

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }

      let payload: unknown;
      try {
        payload = JSON.parse(message.value.toString('utf8')) as unknown;
      } catch {
        return;
      }

      if (!isInventoryAdjustmentRequest(payload)) {
        return;
      }

      await processAdjustment(payload);
    }
  });
}

app.get('/health', (_req: Request, res: Response) => {
  res.status(200).json({
    status: 'UP',
    kafkaConnected
  });
});

app.get('/_meta/inventory/processed-events', (_req: Request, res: Response) => {
  res.status(200).json({
    count: processedEvents.length,
    events: processedEvents
  });
});

app.post('/inventory/adjustments', async (req: Request, res: Response) => {
  if (!isInventoryAdjustmentRequest(req.body)) {
    res.status(400).json({
      error: 'Invalid inventory adjustment request'
    });
    return;
  }

  try {
    const adjusted = await processAdjustment(req.body);
    res.status(202).json(adjusted);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    res.status(500).json({
      error: message
    });
  }
});

void startKafkaConsumer().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  kafkaConnected = false;
  console.error(`kafka consumer startup failed: ${message}`);
});

app.listen(port, host, () => {
  console.log(`inventory-sync-service listening on http://${host}:${port}`);
});
