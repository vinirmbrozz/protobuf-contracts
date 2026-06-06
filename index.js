import { createRequire } from 'module';
const require = createRequire(import.meta.url);

// Importa o arquivo gerado pelo protoc
const messages = require('./gen/node/proto/transaction_pb.js');

// Exporta as classes
export const {
    Transaction,
    PredictiveAnalyzer,
    FinalDecision
} = messages;

export default messages;