import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { handler } from '../src/index'; // Assuming export of functions
import { KinesisStreamEvent } from 'aws-lambda';
import axios from 'axios';
import {
  ErrorDetail,
  validate as originalValidate,
  ValidationError,
} from 'jsonschema';

vi.mock('axios');
vi.mock('jsonschema', () => ({
  validate: vi.fn((data, schema) => ({ valid: true })),
}));

const mockPost = vi.fn();
axios.post = mockPost;

describe('Lambda Function Tests', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    process.env.PUBLISH_URL = 'http://example.com/publish';
    vi.mocked(originalValidate).mockReturnValue({
      valid: true,
      errors: [],
      instance: undefined,
      schema: {},
      propertyPath: '',
      throwError: false,
      disableFormat: false,
      addError: function (detail: string | ErrorDetail): ValidationError {
        throw new Error('Function not implemented.');
      },
    });
  });

  afterEach(() => {
    delete process.env.PUBLISH_URL;
  });

  const baseRecord = {
    kinesis: {
      data: Buffer.from(
        JSON.stringify({
          id: '123',
          partitionKey: 'key123',
          timestamp: Date.now(),
          type: 'booking_completed',
          booking_completed: {
            timestamp: Date.now(),
            product_provider: 'Test Provider',
            orderId: 12345,
          },
        })
      ).toString('base64'),
    },
  };

  it('should process a valid event correctly', async () => {
    const event: KinesisStreamEvent = {
      Records: [baseRecord as any],
    };
    await handler(event);
    expect(mockPost).toHaveBeenCalledOnce();
    expect(mockPost).toHaveBeenCalledWith(process.env.PUBLISH_URL, {
      product_order_id_buyer: 12345,
      timestamp: expect.any(String),
      product_provider_buyer: 'Test Provider',
    });
  });

  it('should not call publishEvent if the event type is not booking_completed', async () => {
    const nonBookingEvent = {
      ...baseRecord,
      kinesis: {
        ...baseRecord.kinesis,
        data: Buffer.from(
          JSON.stringify({
            id: '123',
            partitionKey: 'key123',
            timestamp: Date.now(),
            type: 'other_event',
          })
        ).toString('base64'),
      },
    };
    const event: KinesisStreamEvent = { Records: [nonBookingEvent as any] };
    await handler(event);
    expect(mockPost).not.toHaveBeenCalled();
  });

  it('should catch JSON parse errors and not process further', async () => {
    const invalidJSONRecord = {
      ...baseRecord,
      kinesis: {
        ...baseRecord.kinesis,
        data: Buffer.from('invalid json').toString('base64'),
      },
    };
    const event: KinesisStreamEvent = { Records: [invalidJSONRecord as any] };
    await handler(event);
    expect(mockPost).not.toHaveBeenCalled();
  });

  it('should handle missing booking_completed data', async () => {
    const missingDataRecord = {
      ...baseRecord,
      kinesis: {
        ...baseRecord.kinesis,
        data: Buffer.from(
          JSON.stringify({
            id: '123',
            partitionKey: 'key123',
            timestamp: Date.now(),
            type: 'booking_completed',
          })
        ).toString('base64'),
      },
    };
    const event: KinesisStreamEvent = { Records: [missingDataRecord as any] };
    await handler(event);
    expect(mockPost).not.toHaveBeenCalled();
  });

  it('should validate data and not publish if validation fails', async () => {
    vi.mocked(originalValidate).mockReturnValueOnce({
      valid: false,
      instance: undefined,
      schema: {},
      propertyPath: '',
      errors: [],
      throwError: false,
      disableFormat: false,
      addError: function (detail: string | ErrorDetail): ValidationError {
        throw new Error('Function not implemented.');
      },
    });
    const event: KinesisStreamEvent = { Records: [baseRecord] };
    await handler(event);
    expect(mockPost).not.toHaveBeenCalled();
  });

  it('should catch and log errors during the publish phase', async () => {
    const consoleSpy = vi.spyOn(console, 'error');
    const error = new Error('Network error');
    mockPost.mockRejectedValueOnce(error);
    const event: KinesisStreamEvent = { Records: [baseRecord as any] };
    await handler(event);
    expect(consoleSpy).toHaveBeenCalledWith(
      'Error publishing event:',
      error.message
    );
    consoleSpy.mockRestore();
  });

  it('should log an error if PUBLISH_URL is not defined', async () => {
    delete process.env.PUBLISH_URL;
    const consoleSpy = vi.spyOn(console, 'error');
    const event: KinesisStreamEvent = { Records: [baseRecord as any] };
    await handler(event);
    expect(consoleSpy).toHaveBeenCalledWith('PUBLISH_URL is not defined');
    consoleSpy.mockRestore();
  });
});
