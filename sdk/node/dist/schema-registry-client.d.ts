/**
 * Read-only client for the Confluent Schema Registry REST API.
 * The SDK NEVER registers schemas (that is the registrador's job, out of band).
 * It only resolves/validates ids. Uses Node 18+ native fetch — no extra deps.
 *
 * Endpoints used:
 *   GET /subjects/{subject}/versions/latest  — resolve the subject's schema id
 *   GET /schemas/ids/{id}/versions           — verify an id belongs to a subject
 */
export declare class SchemaRegistryClient {
    private readonly baseUrl;
    private readonly headers;
    /** cache: "<id>|<subject>" confirmed as a registered version of that subject */
    private readonly idSubjectOk;
    constructor(baseUrl: string, apiKey?: string, apiSecret?: string);
    /** Resolve the latest registered schema id for a subject. Throws if absent. */
    latestId(subject: string): Promise<number>;
    /**
     * True iff schema `id` is a registered version of `subject`. A newer version
     * of the same subject is accepted (forward-compat); an id from another subject
     * or unknown to SR is rejected. Caches positive results.
     */
    idBelongsToSubject(id: number, subject: string): Promise<boolean>;
}
//# sourceMappingURL=schema-registry-client.d.ts.map