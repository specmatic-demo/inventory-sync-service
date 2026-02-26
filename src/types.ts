export type InventoryAdjustmentReason =
  | 'ORDER_PLACED'
  | 'ORDER_CANCELLED'
  | 'RETURN_RECEIVED'
  | 'MANUAL_RECONCILIATION';

export type InventoryAdjustmentRequest = {
  requestId: string;
  sku: string;
  adjustmentDelta: number;
  reason: InventoryAdjustmentReason;
  requestedAt: string;
};

export type InventoryAdjusted = {
  eventId: string;
  requestId: string;
  sku: string;
  quantityOnHand: number;
  available: boolean;
  publishedAt: string;
};

export type CatalogAvailability = {
  sku: string;
  available: boolean;
  quantityOnHand: number;
  backorderable: boolean;
};
