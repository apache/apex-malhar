var text = require('../text');
var formatters = require('../formatters');

exports = module.exports = [
    { id: "selector", key: "selected", label: "", select: true, width: 40, lock_width: true },
    { id: "name", key: "name", label: text('filename_label'), filter: "like", sort_value: "a", sort: "string" },
    { id: "modificationTime", label: "mod date", key: "modificationTime", sort: "number", filter: "date", format: "timeStamp" },
    { id: "size", label: "size", key: "size", sort: "number", filter: "number", format: formatters.byteFormatter, width: 80 },
    { id: "owner", label: "owner", key: "owner", sort: "string", filter: "like", width: 60 }
];