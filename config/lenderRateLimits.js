'use strict';

const LENDER_RATE_LIMITS = {
    SML: 100,
    FREO: 120,
    OVLY: 160,
    LendingPlate: 150,
    ZYPE: 120,
    FINTIFI: 150,
    FATAKPAY: 120,
    FATAKPAYPL: 150,
    RAMFINCROP: 150,
    MyMoneyMantra: 100,
    MPOKKET: 150,
    INDIALENDS: 150,
    CRMPaisa: 150,
    CreditPluse: 150,
    CreditSea: 150,
    CreditLinks: 150,
    CreditHaat: 150,
};

const DEFAULT_RATE_LIMIT = 130;

function getRateLimit(lender) {
    const envKey = `RATE_LIMIT_${String(lender).toUpperCase()}`;
    const fromEnv = process.env[envKey];
    if (fromEnv !== undefined && fromEnv !== '') {
        const n = parseInt(fromEnv, 10);
        if (!Number.isNaN(n) && n > 0) return n;
    }
    return LENDER_RATE_LIMITS[lender] || DEFAULT_RATE_LIMIT;
}

module.exports = { LENDER_RATE_LIMITS, DEFAULT_RATE_LIMIT, getRateLimit };
