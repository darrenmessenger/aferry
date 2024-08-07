import { KinesisStreamEvent } from 'aws-lambda';
import axios from 'axios';
import { validate } from 'jsonschema';
import schema from 'external-service/schema.json';

interface BookingCompleted {
  timestamp: number;
  product_provider: string;
  orderId: number;
}

interface IncomingEvent {
  id: string;
  partitionKey: string;
  timestamp: number;
  type: string;
  booking_completed?: BookingCompleted;
}

interface TransformedEvent {
  product_order_id_buyer: number;
  timestamp: string;
  product_provider_buyer: string;
}

interface HttpClient {
  post(url: string, data: any): Promise<void>;
}

class AxiosHttpClient implements HttpClient {
  async post(url: string, data: any): Promise<void> {
    await axios.post(url, data);
  }
}

export function parseEvent(record: any): IncomingEvent | null {
  const payload = Buffer.from(record.kinesis.data, 'base64').toString();
  try {
    return JSON.parse(payload);
  } catch (error) {
    console.error('Error parsing JSON:', (error as Error).message);
    return null;
  }
}

export function transformData(event: IncomingEvent): TransformedEvent | null {
  if (!event.booking_completed) {
    console.error('Missing booking_completed data in event');
    return null;
  }

  return {
    product_order_id_buyer: event.booking_completed.orderId,
    timestamp: new Date(event.booking_completed.timestamp).toISOString(),
    product_provider_buyer: event.booking_completed.product_provider,
  };
}

export function validateData(data: TransformedEvent): boolean {
  const validationResult = validate(data, schema);
  if (!validationResult.valid) {
    const errors = validationResult.errors || [];
    console.error(
      'Validation errors:',
      errors.map((e) => `${e.property} ${e.message}`)
    );
    return false;
  }
  return true;
}

async function publishEvent(
  client: HttpClient,
  url: string,
  data: TransformedEvent
) {
  await client.post(url, data);
  console.log('Event published successfully');
}

export const handler = async (event: KinesisStreamEvent) => {
  const httpClient = new AxiosHttpClient();
  const publishUrl = process.env.PUBLISH_URL;

  if (!publishUrl) {
    console.error('PUBLISH_URL is not defined');
    return;
  }

  for (const record of event.Records) {
    const data = parseEvent(record);
    if (!data) {
      continue;
    }

    if (data.type !== 'booking_completed') {
      continue;
    }

    const transformedData = transformData(data);
    if (!transformedData) {
      console.error('Transformation failed for a booking_completed event');
      continue;
    }

    if (validateData(transformedData)) {
      try {
        await publishEvent(httpClient, publishUrl, transformedData);
      } catch (error) {
        console.error('Error publishing event:', (error as Error).message);
      }
    } else {
      console.error('Invalid event data, not publishing');
    }
  }
};
