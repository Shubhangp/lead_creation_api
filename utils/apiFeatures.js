class APIFeatures {
    constructor(query, queryString) {
        this.query = query;
        this.queryString = queryString;
    }
  
    filter() {
        const queryObj = { ...this.queryString };
        const excludedFields = ['page', 'sort', 'limit', 'fields'];
        excludedFields.forEach(el => delete queryObj[el]);
    
        // 1B) Advanced filtering
        let queryStr = JSON.stringify(queryObj);
        queryStr = queryStr.replace(/\b(gte|gt|lte|lt)\b/g, match => `$${match}`);
    
        this.query = this.query.find(JSON.parse(queryStr));
    
        return this;
    }
  
    sort() {
        if (this.queryString.sort) {
            const sortBy = this.queryString.sort.split(',').join(' ');
            this.query = this.query.sort(sortBy);
        } else {
            // Prefer sorting by createdAt only if index exists on createdAt
            this.query = this.query.sort('-createdAt');
        }
    
        return this;
    }
  
    limitFields() {
        if (this.queryString.fields) {
            const fields = this.queryString.fields.split(',').join(' ');
            this.query = this.query.select(fields);
        } else {
            this.query = this.query.select('-__v');
        }
    
        return this;
    }
  
    paginate() {
        const page = this.queryString.page * 1 || 1;
        // Cap limit to prevent memory blowups
        const requestedLimit = this.queryString.limit * 1 || 100;
        const limit = Math.min(requestedLimit, 200);
        const skip = (page - 1) * limit;
    
        this.query = this.query.skip(skip).limit(limit).lean();
    
        return this;
    }
}
module.exports = APIFeatures;  