"use strict";
/**
 * Read-only client for the Confluent Schema Registry REST API.
 * The SDK NEVER registers schemas (that is the registrador's job, out of band).
 * It only resolves/validates ids. Uses Node 18+ native fetch — no extra deps.
 *
 * Endpoints used:
 *   GET /subjects/{subject}/versions/latest  — resolve the subject's schema id
 *   GET /schemas/ids/{id}/versions           — verify an id belongs to a subject
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.SchemaRegistryClient = void 0;
const ACCEPT = 'application/vnd.schemaregistry.v1+json';
class SchemaRegistryClient {
    constructor(baseUrl, apiKey, apiSecret) {
        /** cache: "<id>|<subject>" confirmed as a registered version of that subject */
        this.idSubjectOk = new Set();
        this.baseUrl = baseUrl.replace(/\/$/, '');
        this.headers = { Accept: ACCEPT };
        if (apiKey && apiSecret) {
            this.headers['Authorization'] =
                'Basic ' + Buffer.from(`${apiKey}:${apiSecret}`).toString('base64');
        }
    }
    /** Resolve the latest registered schema id for a subject. Throws if absent. */
    async latestId(subject) {
        const res = await fetch(`${this.baseUrl}/subjects/${encodeURIComponent(subject)}/versions/latest`, { headers: this.headers });
        if (res.status === 404) {
            throw new Error(`subject '${subject}' is not registered in Schema Registry`);
        }
        if (!res.ok) {
            throw new Error(`SR resolve '${subject}': HTTP ${res.status} ${await res.text()}`);
        }
        const { id } = (await res.json());
        return id;
    }
    /**
     * True iff schema `id` is a registered version of `subject`. A newer version
     * of the same subject is accepted (forward-compat); an id from another subject
     * or unknown to SR is rejected. Caches positive results.
     */
    async idBelongsToSubject(id, subject) {
        const key = `${id}|${subject}`;
        if (this.idSubjectOk.has(key))
            return true;
        const res = await fetch(`${this.baseUrl}/schemas/ids/${id}/versions`, {
            headers: this.headers,
        });
        if (res.status === 404)
            return false;
        if (!res.ok) {
            throw new Error(`SR verify id=${id}: HTTP ${res.status} ${await res.text()}`);
        }
        const pairs = (await res.json());
        const ok = pairs.some((p) => p.subject === subject);
        if (ok)
            this.idSubjectOk.add(key);
        return ok;
    }
}
exports.SchemaRegistryClient = SchemaRegistryClient;
//# sourceMappingURL=schema-registry-client.js.map