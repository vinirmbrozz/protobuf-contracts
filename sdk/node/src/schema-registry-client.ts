/**
 * Minimal HTTP client for Confluent Schema Registry REST API.
 * Uses Node 18+ native fetch — no extra dependencies.
 *
 * Only the two endpoints used by the serde lib are implemented:
 *   POST /subjects/{subject}/versions  — register a schema, get its ID
 *   GET  /schemas/ids/{id}             — verify a schema ID exists
 */

const JSON_CONTENT = 'application/vnd.schemaregistry.v1+json';

export class SchemaRegistryClient {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;

  /**
   * Cache: schema_id → subject name.
   * Populated on registration and on successful SR lookups.
   */
  private readonly idCache = new Map<number, string>();

  constructor(baseUrl: string, apiKey?: string, apiSecret?: string) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.headers = {
      'Content-Type': JSON_CONTENT,
      Accept: JSON_CONTENT,
    };
    if (apiKey && apiSecret) {
      const creds = Buffer.from(`${apiKey}:${apiSecret}`).toString('base64');
      this.headers['Authorization'] = `Basic ${creds}`;
    }
  }

  /**
   * Register a PROTOBUF schema under `<topic>-value` and return the schema ID.
   * If the identical schema is already registered, SR returns the existing ID
   * (idempotent). Caches the ID locally.
   */
  async registerSchema(subject: string, protoContent: string): Promise<number> {
    const res = await fetch(`${this.baseUrl}/subjects/${subject}/versions`, {
      method: 'POST',
      headers: this.headers,
      body: JSON.stringify({ schemaType: 'PROTOBUF', schema: protoContent }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`SR registration failed for subject '${subject}': ${res.status} ${body}`);
    }
    const { id } = (await res.json()) as { id: number };
    this.idCache.set(id, subject);
    return id;
  }

  /**
   * Verify a schema ID exists in SR. Returns the subject it belongs to,
   * or null when the ID is unknown (404). Caches successful lookups.
   */
  async verifySchemaId(schemaId: number): Promise<string | null> {
    if (this.idCache.has(schemaId)) return this.idCache.get(schemaId)!;

    const res = await fetch(`${this.baseUrl}/schemas/ids/${schemaId}`, {
      headers: this.headers,
    });
    if (res.status === 404) return null;
    if (!res.ok) {
      throw new Error(`SR lookup failed for schema_id ${schemaId}: ${res.status}`);
    }
    // SR returns the schema content; we only need to confirm it exists.
    // Store a placeholder so repeated unknown IDs still hit the cache.
    this.idCache.set(schemaId, `__remote__${schemaId}`);
    return this.idCache.get(schemaId)!;
  }

  /** True when the schema ID is already in the local cache (no network call). */
  isCached(schemaId: number): boolean {
    return this.idCache.has(schemaId);
  }
}
