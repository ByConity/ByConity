(function() {
    "use strict";
    var __webpack_modules__ = {
        5378: function(__unused_webpack_module, __unused_webpack___webpack_exports__, __webpack_require__) {
            var dist = __webpack_require__(1087);
            var react_router_dist = __webpack_require__(7689);
            var client = __webpack_require__(1250);
            var react = __webpack_require__(2791);
            var objectSpread2 = __webpack_require__(1413);
            var slicedToArray = __webpack_require__(9439);
            var classCallCheck = __webpack_require__(5671);
            var createClass = __webpack_require__(3144);
            var message = __webpack_require__(3695);
            var loglevel = __webpack_require__(2895);
            var loglevel_default = __webpack_require__.n(loglevel);
            var EngineName = "cnch";
            function canConnect(auth) {
                var controller = new AbortController;
                setTimeout((function() {
                    return controller.abort();
                }), 2e3);
                return fetch("http://".concat(auth.ip, ":").concat(auth.port, "/ping"), {
                    method: "GET",
                    mode: "no-cors",
                    signal: controller.signal
                }).then((function(ret) {
                    return true;
                })).catch((function(e) {
                    loglevel_default().error(e);
                    return false;
                }));
            }
            function executeQuery(sql, auth, dialect_type) {
                loglevel_default().info("executing sql: ", sql);
                var url = "http://".concat(auth.ip, ":").concat(auth.port, "/?add_http_cors_header=1");
                var format = "JSON";
                url += "&default_format=".concat(format, "&max_result_rows=100000&max_result_bytes=10000000&result_overflow_mode=throw");
                if (dialect_type) url += "&dialect_type=".concat(dialect_type);
                if (auth.user !== "") url += "&user=".concat(auth.user);
                if (auth.password !== "") url += "&password=".concat(auth.password);
                loglevel_default().debug("request url: ", url);
                var controller = new AbortController;
                setTimeout((function() {
                    return controller.abort();
                }), 6e4);
                return fetch(url, {
                    method: "POST",
                    body: sql,
                    signal: controller.signal,
                    credentials: "omit"
                }).then((function(response) {
                    if (response.ok && format === "JSON") return response.json(); else return response.text();
                })).then((function(ret) {
                    if (typeof ret === "object" || format !== "JSON") return ret; else {
                        if (typeof ret === "string" && ret.indexOf("Code: 396") !== -1) {
                            throw Error(ret + " Please try to set a smaller max_threads");
                        } else {
                            throw Error(ret);
                        }
                    }
                })).catch((function(err) {
                    if (err instanceof DOMException) {
                        throw new DOMException("Request is aborted due to timeout. Check the network connection or reduce the requested data size.");
                    } else {
                        throw err;
                    }
                }));
            }
            function tryLogin(auth) {
                return executeQuery("SELECT name, value from system.build_options where name='VERSION_SCM' or name='VERSION_FULL'", auth).then((function(ret) {
                    if (ret.rows === 0) {
                        throw new Error("Cannot find the engine version or scm from system.build_options");
                    }
                    var full = "";
                    var scm = "";
                    ret.data.forEach((function(row) {
                        if (row.name === "VERSION_FULL") full = row.value;
                        if (row.name === "VERSION_SCM") scm = row.value;
                    }));
                    if (scm === "1.0.0.0") scm = "".concat(EngineName, "-dev");
                    return new Engine(auth, full, scm);
                }));
            }
            var Engine = function() {
                function Engine(auth, fullVersion, scmVersion) {
                    var _this = this;
                    (0, classCallCheck.Z)(this, Engine);
                    this.name = EngineName;
                    this.auth = auth;
                    this.fullVersion = fullVersion;
                    this.scmVersion = scmVersion;
                    this.dialect_type = "CLICKHOUSE";
                    var LocalTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
                    if (LocalTimeZone === undefined) {
                        message.ZP.warning("Failed to get local time zone; Going to use UTC+8");
                        LocalTimeZone = "Asia/Singapore";
                    }
                    this.toServerDateTime = function(date_time) {
                        return "toDateTime('".concat(date_time, "', '").concat(LocalTimeZone, "')");
                    };
                    this.toServerDate = function(date_time) {
                        return "toDate(".concat(_this.toServerDateTime(date_time), ")");
                    };
                    this.toLocalTZ = function(column, alias) {
                        return "toTimeZone(".concat(column, ", '").concat(LocalTimeZone, "') as ") + (alias ? alias : column);
                    };
                    this.execute = this.execute.bind(this);
                    this.getExcludeSystemTablePredicate = this.getExcludeSystemTablePredicate.bind(this);
                    this.getFlameGraphURL = this.getFlameGraphURL.bind(this);
                    this.settings = new Map;
                    executeQuery("select name, value from system.settings", this.auth).then((function(json) {
                        json.data.forEach((function(row) {
                            _this.settings[row.name] = row.value;
                        }));
                    }));
                }
                (0, createClass.Z)(Engine, [ {
                    key: "execute",
                    value: function execute(select, from, where) {
                        var sql = "select ".concat(select, ", 0 as shard from ").concat(from) + (where ? " where ".concat(where) : "");
                        if (this.auth.cluster) {
                            from = "cnch(".concat(this.auth.cluster, ", ").concat(from, ")");
                            var vw_sql = "select ".concat(select, ", _shard_num as shard from ").concat(from) + (where ? " where ".concat(where) : "");
                            sql = "".concat(sql, " union all ").concat(vw_sql);
                        }
                        return executeQuery(sql, this.auth, this.dialect_type).then((function(json) {
                            return json.data;
                        }));
                    }
                }, {
                    key: "getExcludeSystemTablePredicate",
                    value: function getExcludeSystemTablePredicate() {
                        return " has(databases, 'system')=0 ";
                    }
                }, {
                    key: "getFlameGraphURL",
                    value: function getFlameGraphURL(where) {
                        var _this2 = this;
                        where = "".concat(where, " and trace_type=1 ");
                        return this.execute("event_date", "system.trace_log", where + " limit 10").then((function(rows) {
                            if (rows.length >= 10) {
                                var from = "system.trace_log WHERE ".concat(where);
                                if (_this2.auth.cluster) {
                                    from = "SELECT trace FROM ".concat(from, " UNION ALL SELECT trace FROM cnch(").concat(_this2.auth.cluster, ", system.trace_log) WHERE ").concat(where);
                                }
                                var sql = "SELECT arrayStringConcat(arrayReverse(arrayMap(x -> demangle(addressToSymbol(x)), trace)), ';') AS stack, " + "count() AS samples FROM (".concat(from, ") ") + "GROUP BY trace FORMAT CustomSeparated SETTINGS allow_introspection_functions=1,format_custom_field_delimiter=' '";
                                var url = "http://".concat(_this2.auth.ip, ":").concat(_this2.auth.port, "?add_http_cors_header=1&query=").concat(sql);
                                if (_this2.auth.user !== "default") url += "&user=".concat(_this2.auth.user);
                                if (_this2.auth.password !== "") {
                                    url += "&password=".concat(_this2.auth.password);
                                }
                                loglevel_default().debug(url);
                                return encodeURIComponent(url);
                            } else {
                                throw new Error("No trace records in the trace log. Check the start and end datetime, or query's sampling interval/frequency.");
                            }
                        }));
                    }
                } ]);
                return Engine;
            }();
            var jsx_runtime = __webpack_require__(184);
            var AuthContext = (0, react.createContext)(null);
            function AuthProvider(_ref) {
                var children = _ref.children;
                var _useState = (0, react.useState)(null), _useState2 = (0, slicedToArray.Z)(_useState, 2), auth = _useState2[0], setAuth = _useState2[1];
                var signIn = function signIn(newAuth, callback) {
                    return canConnect(newAuth).then((function(ok) {
                        if (ok) {
                            return tryLogin(newAuth).then((function(engine) {
                                setAuth((0, objectSpread2.Z)((0, objectSpread2.Z)({}, newAuth), {}, {
                                    engine: engine,
                                    loggedIn: true
                                }));
                                return setTimeout(callback, 100);
                            }));
                        } else {
                            throw Error("Cannot connect to the server; please make sure the server is up and accessible (e.g., via port-forward)");
                        }
                    }));
                };
                var signOut = function signOut(callback) {
                    setAuth(null);
                    callback();
                };
                var value = {
                    auth: auth,
                    setAuth: setAuth,
                    signIn: signIn,
                    signOut: signOut
                };
                return (0, jsx_runtime.jsx)(AuthContext.Provider, {
                    value: value,
                    children: children
                });
            }
            function useAuth() {
                return (0, react.useContext)(AuthContext);
            }
            function RequireAuth(_ref2) {
                var children = _ref2.children;
                var _useAuth = useAuth(), auth = _useAuth.auth;
                var location = (0, react_router_dist.TH)();
                if (!auth) {
                    loglevel_default().debug("redirect to login");
                    return (0, jsx_runtime.jsx)(react_router_dist.Fg, {
                        to: "/login",
                        state: {
                            from: location
                        },
                        replace: true
                    });
                }
                return children;
            }
            var createForOfIteratorHelper = __webpack_require__(7762);
            var spin = __webpack_require__(7083);
            var drawer = __webpack_require__(8885);
            var es_form = __webpack_require__(1541);
            var input = __webpack_require__(177);
            var es_button = __webpack_require__(7309);
            var moment = __webpack_require__(2426);
            var moment_default = __webpack_require__.n(moment);
            var tooltip = __webpack_require__(5945);
            function getColNameOrAlias(colName) {
                if (colName.split(" ").length > 1) return colName.split(" ").at(-1); else return colName;
            }
            var TimeFormat = "YYYY-MM-DD HH:mm:ss.SSSSSS";
            function numberWithComma(x) {
                if (x !== null && x !== undefined) {
                    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                } else {
                    return null;
                }
            }
            function convertTimeUnit(x) {
                if (x >= 1e6) return "".concat((x / 1e6).toFixed(1), "s"); else if (x >= 1e3) return "".concat((x / 1e3).toFixed(1), "ms"); else return "".concat(Math.floor(x), "us");
            }
            function convertUnit(x) {
                var isSize = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
                x = parseInt(x);
                if (x >= 1e9) {
                    return "".concat(Math.floor(x / 1e8) / 10) + (isSize ? "G" : "Bn");
                } else if (x >= 1e6) return "".concat(Math.floor(x / 1e5) / 10, "M"); else if (x >= 1e3) return "".concat(Math.floor(x / 100) / 10, "K"); else return String(x);
            }
            function prepareTable(metrics, rows, keyColumn) {
                var colsDef = [];
                var data = [];
                if (rows.length === 0) {
                    loglevel_default().warn("empty rows to create table");
                    return {
                        columns: colsDef,
                        dataSource: data
                    };
                }
                metrics.forEach((function(col) {
                    if (col[1]) {
                        var dataIdx = getColNameOrAlias(col[0]);
                        var colDef = {
                            title: col[2] ? (0, jsx_runtime.jsx)(tooltip.Z, {
                                title: col[2],
                                children: col[1]
                            }) : col[1],
                            dataIndex: dataIdx
                        };
                        if (col.length < 4) {
                            throw Error("Each column must have at least 4 fields for column, " + "column_name, display_name, display_hint, sort_type [, tooltip=true|false]" + "There are only ".concat(col.length, " fields"));
                        }
                        if (col[3].toUpperCase() === "DATE") {
                            colDef.sorter = {
                                compare: function compare(a, b) {
                                    return moment_default()(a[dataIdx], TimeFormat) - moment_default()(b[dataIdx], TimeFormat);
                                },
                                multiple: 1
                            };
                        } else if (col[3].toUpperCase().includes("INT")) {
                            colDef.render = function(_, record) {
                                return numberWithComma(record[dataIdx]);
                            };
                            colDef.sorter = {
                                compare: function compare(a, b) {
                                    return a[dataIdx] - b[dataIdx];
                                },
                                multiple: 1
                            };
                        } else if (col[3].toUpperCase() === "STRING") {
                            if (col.length >= 5 && col[4] === true) {
                                colDef.ellipsis = {
                                    showTitle: false
                                };
                                colDef.render = function(txt) {
                                    return (0, jsx_runtime.jsx)(tooltip.Z, {
                                        placement: "topLeft",
                                        title: (0, jsx_runtime.jsx)("pre", {
                                            children: txt
                                        }),
                                        overlayStyle: {
                                            maxWidth: "50vw"
                                        },
                                        children: txt
                                    });
                                };
                            }
                        }
                        colsDef.push(colDef);
                    }
                }));
                if (keyColumn) {
                    rows.forEach((function(row) {
                        row.key = row[keyColumn];
                    }));
                } else {
                    rows.forEach((function(row, idx) {
                        row.key = idx;
                    }));
                }
                return {
                    columns: colsDef,
                    dataSource: rows
                };
            }
            function fillColor(title, time, color_exchange) {
                var kDanger = 5e6;
                var kWarning = 1e6;
                var kInfo = 5e5;
                var kGood = 1e5;
                if ((title.indexOf("Exchange") !== -1 || title.startsWith("MultiPartition")) && !color_exchange) return "white";
                if (time > kDanger) return "red"; else if (time > kWarning) return "orange"; else if (time > kInfo) return "yellow2"; else if (time > kGood) return "darkolivegreen2"; else return "white";
            }
            var collapse = __webpack_require__(63);
            var es_table = __webpack_require__(7886);
            var divider = __webpack_require__(1333);
            var src = __webpack_require__(4692);
            var d3_graphviz = __webpack_require__(7059);
            var Panel = collapse.Z.Panel;
            function NodeDetails(_ref) {
                var details = _ref.details, setDetails = _ref.setDetails;
                var onClose = function onClose() {
                    setDetails({
                        sections: []
                    });
                };
                return (0, jsx_runtime.jsx)(drawer.Z, {
                    title: details.title,
                    placement: "left",
                    closable: false,
                    size: "large",
                    onClose: onClose,
                    open: details.sections.length,
                    children: (0, jsx_runtime.jsx)(collapse.Z, {
                        defaultActiveKey: [ "0" ],
                        children: details.sections.map((function(section, pidx) {
                            return (0, jsx_runtime.jsx)(Panel, {
                                header: section.header,
                                children: (0, jsx_runtime.jsxs)("ul", {
                                    children: [ section.fields && section.fields.map((function(field, idx) {
                                        return (0, jsx_runtime.jsxs)("li", {
                                            children: [ (0, jsx_runtime.jsx)("span", {
                                                children: field[0]
                                            }), field.length > 1 && (0, jsx_runtime.jsxs)(jsx_runtime.Fragment, {
                                                children: [ (0, jsx_runtime.jsx)("span", {
                                                    children: ":"
                                                }), (0, jsx_runtime.jsx)("span", {
                                                    style: {
                                                        marginLeft: 5
                                                    },
                                                    children: field[1]
                                                }) ]
                                            }) ]
                                        }, "field-".concat(idx));
                                    })), section.table && (0, jsx_runtime.jsx)(es_table.Z, {
                                        size: "small",
                                        scroll: {
                                            x: section.scrollX
                                        },
                                        pagination: false,
                                        columns: section.table.columns,
                                        dataSource: section.table.dataSource
                                    }), section.tables && section.tables.map((function(tbl) {
                                        return (0, jsx_runtime.jsxs)(jsx_runtime.Fragment, {
                                            children: [ (0, jsx_runtime.jsx)(es_table.Z, {
                                                size: "small",
                                                scroll: {
                                                    x: section.scrollX
                                                },
                                                pagination: false,
                                                columns: tbl.columns,
                                                dataSource: tbl.dataSource
                                            }), (0, jsx_runtime.jsx)(divider.Z, {}) ]
                                        });
                                    })) ]
                                })
                            }, pidx);
                        }))
                    })
                });
            }
            function resetGraph() {
                (0, d3_graphviz.f)("#graphcanvas").resetZoom();
            }
            function Graph(_ref2) {
                var graph_dot = _ref2.graph_dot, node_details_func = _ref2.node_details_func;
                var _useState = (0, react.useState)({
                    sections: []
                }), _useState2 = (0, slicedToArray.Z)(_useState, 2), details = _useState2[0], setDetails = _useState2[1];
                loglevel_default().debug("dot: ", graph_dot);
                (0, react.useEffect)((function() {
                    var container = src.Ys("#graphcanvas");
                    var width = container.node().clientWidth;
                    var height = container.node().clientHeight;
                    loglevel_default().debug("container size: ", width, height);
                    try {
                        (0, d3_graphviz.f)("#graphcanvas", {
                            useWorker: false,
                            fit: true,
                            width: width,
                            height: height,
                            zoomScaleExtent: [ .1, 20 ]
                        }).renderDot(graph_dot).on("end", (function() {}));
                    } catch (err) {
                        message.ZP.error(err);
                    }
                    var inner = src.Ys("#graphcanvas").select("g").selectAll("g");
                    inner.on("click", (function(_, node) {
                        if (node_details_func) {
                            setDetails(node_details_func(node));
                        }
                    }));
                }), [ graph_dot ]);
                return (0, jsx_runtime.jsxs)(jsx_runtime.Fragment, {
                    children: [ (0, jsx_runtime.jsx)("div", {
                        className: "drawer",
                        children: (0, jsx_runtime.jsx)(NodeDetails, {
                            details: details,
                            setDetails: setDetails
                        })
                    }), (0, jsx_runtime.jsx)("div", {
                        id: "graphcanvas",
                        style: {
                            width: "100%",
                            height: "85vh",
                            marginTop: 10
                        }
                    }) ]
                });
            }
            var lodash = __webpack_require__(763);
            function toDotString(fragments, extra_edges) {
                var edge_dots = [];
                var frag_dots = fragments.map((function(fragment) {
                    var pipe_dots = fragment.pipelines.map((function(pipeline) {
                        var proc_dots = pipeline.processors.map((function(proc) {
                            var fill_color = fillColor(proc.name, proc.time);
                            return "".concat(proc.id, " [id=").concat(proc.id, ' label="{').concat(proc.name, "|{\ud83d\udd50 ").concat(convertTimeUnit(proc.time), "|x").concat(proc.parallelism, '}}", shape=record, style="rounded, filled", fillcolor=').concat(fill_color, "]");
                        }));
                        for (var i = 1; i < pipeline.processors.length; i++) {
                            var from = pipeline.processors[i];
                            var to = pipeline.processors[i - 1];
                            edge_dots.push("".concat(from.id, "->").concat(to.id, '[label="').concat(convertUnit(from.output_rows), '"]'));
                        }
                        return "subgraph cluster_pipeline_".concat(pipeline.id, ' { label="Pipeline ').concat(pipeline.id, '" style=dashed; ').concat(proc_dots.join("\n"), "}");
                    }));
                    return "subgraph cluster_fragment_".concat(fragment.id, ' { label="Fragment ').concat(fragment.id, '" fontsize=20; ').concat(pipe_dots.join("\n"), " }");
                }));
                extra_edges.forEach((function(edge) {
                    if (edge.to === null || edge.from === null) {
                        loglevel_default().debug("edge with null endpoint:", edge);
                    } else {
                        edge_dots.push("".concat(edge.from.id, "->").concat(edge.to.id, '[label="').concat(convertUnit(edge.from.output_rows), '"]'));
                    }
                }));
                return 'digraph { rankdir="BT" '.concat(frag_dots.join("\n"), "  ").concat(edge_dots.join("\n"), " }");
            }
            function getFragmentPattern(line) {
                return line.match(/^\s+Fragment (\d+):/);
            }
            function getPipelinePattern(line) {
                return line.match(/^\s+Pipeline \(id=(\d+)\):/);
            }
            function getProcessorPattern(line) {
                return line.match(/^\s+([\w-]+)\s+\(\w*plan_node_id=(-?\d+)\)/);
            }
            function parseProperties(line_id, lines, properties) {
                var isProperty = function isProperty(line) {
                    return !(getFragmentPattern(line) || getPipelinePattern(line) || getProcessorPattern(line));
                };
                var skipBlankLines = function skipBlankLines() {
                    while (line_id < lines.length && !lines[line_id].trim()) {
                        line_id += 1;
                    }
                };
                while (line_id < lines.length && isProperty(lines[line_id])) {
                    properties.push(lines[line_id].trim());
                    line_id += 1;
                }
                skipBlankLines();
                return line_id;
            }
            function parseProcessor(line_id, lines, processor) {
                processor.properties = [];
                line_id = parseProperties(line_id, lines, processor.properties);
                var _iterator = (0, createForOfIteratorHelper.Z)(processor.properties), _step;
                try {
                    for (_iterator.s(); !(_step = _iterator.n()).done; ) {
                        var prop = _step.value;
                        if (prop.indexOf("- OperatorTotalTime") !== -1 || prop.indexOf("__MAX_OF_OperatorTotalTime") !== -1) {
                            var m = null;
                            if ((m = prop.match(/(\d+)s(\d*)/)) !== null) {
                                processor.time = (0, lodash.parseInt)(m[1]) * 1e6 + (0, lodash.parseInt)(m[2] === null ? 0 : m[2]) * 1e3;
                            } else {
                                processor.time = parseFloat(prop.split(":").at(-1));
                                if (prop.endsWith("ms")) processor.time *= 1e3; else if (prop.endsWith("ns")) processor.time = 0; else if (!prop.endsWith("us")) processor.time *= 1e6;
                            }
                        }
                        if (prop.indexOf("- Table:") !== -1) {
                            processor.table = prop.split(":").at(-1);
                        }
                        var match = null;
                        if ((match = prop.match(/- PullRowNum: .+ \((\d+)\)/)) !== null) {
                            processor.input_rows = (0, lodash.parseInt)(match[1]);
                        } else if ((match = prop.match(/- PullRowNum: (\d+)/)) !== null) {
                            processor.input_rows = (0, lodash.parseInt)(match[1]);
                        }
                        if ((match = prop.match(/- PushRowNum: .+ \((\d+)\)/)) !== null) {
                            processor.output_rows = (0, lodash.parseInt)(match[1]);
                        } else if ((match = prop.match(/- PushRowNum: (\d+)/)) !== null) {
                            processor.output_rows = (0, lodash.parseInt)(match[1]);
                        }
                    }
                } catch (err) {
                    _iterator.e(err);
                } finally {
                    _iterator.f();
                }
                processor.parallelism = processor.pipeline.parallelism;
                return line_id;
            }
            function parsePipeline(line_id, lines, pipeline, extra_edges) {
                pipeline.properties = [];
                line_id = parseProperties(line_id, lines, pipeline.properties);
                pipeline.properties.forEach((function(prop) {
                    if (prop.indexOf("TotalDegreeOfParallelism") !== -1) {
                        pipeline.parallelism = prop.split(":").at(-1);
                    }
                }));
                pipeline.processors = [];
                while (line_id < lines.length) {
                    var match = null;
                    if (lines[line_id].trim() === "RESULT_SINK:") {
                        var processor = {
                            name: "RESULT_SINK",
                            id: "line".concat(line_id),
                            node_id: -1e3,
                            pipeline: pipeline
                        };
                        line_id = parseProcessor(line_id + 1, lines, processor);
                        pipeline.processors.push(processor);
                    } else if ((match = getProcessorPattern(lines[line_id])) !== null) {
                        var _processor = {
                            name: match[1].trim(),
                            id: "line".concat(line_id),
                            node_id: (0, lodash.parseInt)(match[2]),
                            pipeline: pipeline
                        };
                        var edge_id = _processor.node_id;
                        if (_processor.name.startsWith("LOCAL")) {
                            edge_id = "".concat(pipeline.fragment.id, "-").concat(_processor.node_id);
                        }
                        if (_processor.name.endsWith("_SINK") || _processor.name.endsWith("_BUILD")) {
                            if (!extra_edges.has(edge_id)) {
                                extra_edges.set(edge_id, {
                                    from: null,
                                    to: null
                                });
                            }
                            extra_edges.get(edge_id).from = _processor;
                        } else if (_processor.name.endsWith("_SOURCE") || _processor.name.endsWith("_PROBE")) {
                            if (!extra_edges.has(edge_id)) {
                                extra_edges.set(edge_id, {
                                    from: null,
                                    to: null
                                });
                            }
                            extra_edges.get(edge_id).to = _processor;
                        }
                        line_id = parseProcessor(line_id + 1, lines, _processor);
                        pipeline.processors.push(_processor);
                    } else {
                        break;
                    }
                }
                return line_id;
            }
            function parseFragment(line_id, lines, fragment, extra_edges) {
                fragment.properties = [];
                fragment.pipelines = [];
                line_id = parseProperties(line_id, lines, fragment.properties);
                var match = null;
                while (line_id < lines.length && (match = getPipelinePattern(lines[line_id]))) {
                    var pipeline = {
                        id: match[1],
                        fragment: fragment
                    };
                    line_id = parsePipeline(line_id + 1, lines, pipeline, extra_edges);
                    fragment.pipelines.push(pipeline);
                }
                return line_id;
            }
            function parsePlan(profile_str) {
                var lines = profile_str.split("\n");
                var line_id = 0;
                var fragments = [];
                var extra_edges = new Map;
                while (line_id < lines.length) {
                    var match = null;
                    if ((match = getFragmentPattern(lines[line_id])) !== null) {
                        var fragment = {
                            id: (0, lodash.parseInt)(match[1])
                        };
                        line_id = parseFragment(line_id + 1, lines, fragment, extra_edges);
                        fragments.push(fragment);
                    } else {
                        loglevel_default().warn("Expect fragment string at line ".concat(line_id, ", but got ").concat(lines[line_id]));
                        line_id++;
                    }
                }
                return {
                    fragments: fragments,
                    extra_edges: extra_edges
                };
            }
            function prepareProcessorDetails(processor) {
                return {
                    sections: [ {
                        header: "Basics",
                        fields: [ [ "Name", processor.name ], [ "Plan node id", processor.node_id ], [ "Pipeline", processor.pipeline.id ], [ "Fragment", processor.pipeline.fragment.id ] ]
                    }, {
                        header: "Processor",
                        fields: processor.properties.map((function(prop) {
                            return [ prop ];
                        }))
                    }, {
                        header: "Pipeline",
                        fields: processor.pipeline.properties.map((function(prop) {
                            return [ prop ];
                        }))
                    }, {
                        header: "Fragment",
                        fields: processor.pipeline.fragment.properties.map((function(prop) {
                            return [ prop ];
                        }))
                    } ]
                };
            }
            function StarRocks() {
                var _useState = (0, react.useState)(true), _useState2 = (0, slicedToArray.Z)(_useState, 2), openDrawer = _useState2[0], setOpenDrawer = _useState2[1];
                var _useState3 = (0, react.useState)(false), _useState4 = (0, slicedToArray.Z)(_useState3, 2), loading = _useState4[0], setLoading = _useState4[1];
                var _useState5 = (0, react.useState)(new Map), _useState6 = (0, slicedToArray.Z)(_useState5, 2), processors = _useState6[0], setProcessors = _useState6[1];
                var _useState7 = (0, react.useState)(""), _useState8 = (0, slicedToArray.Z)(_useState7, 2), graphDot = _useState8[0], setGraphDot = _useState8[1];
                var onSubmit = function onSubmit(values) {
                    setLoading(true);
                    setOpenDrawer(false);
                    var _parsePlan = parsePlan(values.profileStr), fragments = _parsePlan.fragments, extra_edges = _parsePlan.extra_edges;
                    var processor_map = new Map;
                    fragments.forEach((function(fragment) {
                        fragment.pipelines.forEach((function(pipeline) {
                            pipeline.processors.forEach((function(processor) {
                                processor_map.set(processor.id, processor);
                            }));
                        }));
                    }));
                    setProcessors(processor_map);
                    var dotstr = toDotString(fragments, extra_edges);
                    setGraphDot(dotstr);
                    setLoading(false);
                };
                var node_details_func = function node_details_func(node) {
                    loglevel_default().debug("clicked on ", node.key);
                    return prepareProcessorDetails(processors.get(node.key));
                };
                var onClose = function onClose() {
                    setOpenDrawer(false);
                };
                return (0, jsx_runtime.jsxs)("div", {
                    children: [ (0, jsx_runtime.jsx)(spin.Z, {
                        spinning: loading,
                        size: "large",
                        children: (0, jsx_runtime.jsx)(Graph, {
                            graph_dot: graphDot,
                            node_details_func: node_details_func
                        })
                    }), (0, jsx_runtime.jsx)(drawer.Z, {
                        placement: "right",
                        closable: false,
                        size: "large",
                        onClose: onClose,
                        open: openDrawer,
                        children: (0, jsx_runtime.jsxs)(es_form.Z, {
                            className: "login-form",
                            onFinish: onSubmit,
                            layout: "inline",
                            children: [ (0, jsx_runtime.jsx)(es_form.Z.Item, {
                                name: "profileStr",
                                children: (0, jsx_runtime.jsx)(input.Z.TextArea, {
                                    style: {
                                        height: "85vh",
                                        width: "45vw"
                                    },
                                    placeholder: "Profile generated by 'set is_report_success=true'; displayed on StarRocks's web portal"
                                })
                            }), (0, jsx_runtime.jsx)(es_form.Z.Item, {
                                children: (0, jsx_runtime.jsx)(es_button.Z, {
                                    type: "primary",
                                    htmlType: "submit",
                                    children: "Submit"
                                })
                            }) ]
                        })
                    }) ]
                });
            }
            var modal = __webpack_require__(3801);
            var dropdown = __webpack_require__(7706);
            var space = __webpack_require__(3099);
            var es_switch = __webpack_require__(5581);
            var typography = __webpack_require__(5485);
            var RightOutlined = __webpack_require__(1938);
            var DownOutlined = __webpack_require__(7295);
            var UserOutlined = __webpack_require__(9529);
            var LockOutlined = __webpack_require__(8999);
            var localforage = __webpack_require__(1842);
            var localforage_default = __webpack_require__.n(localforage);
            var AuthKey = "!@profiler/auth";
            var Text = typography.Z.Text, Link = typography.Z.Link, Title = typography.Z.Title;
            function Login() {
                var _location$state, _location$state$from;
                var _useParams = (0, react_router_dist.UO)(), instruction = _useParams.instruction;
                var _useState = (0, react.useState)(false), _useState2 = (0, slicedToArray.Z)(_useState, 2), verifying = _useState2[0], setVerifying = _useState2[1];
                var _useState3 = (0, react.useState)([]), _useState4 = (0, slicedToArray.Z)(_useState3, 2), clusters = _useState4[0], setClusters = _useState4[1];
                var _useState5 = (0, react.useState)(), _useState6 = (0, slicedToArray.Z)(_useState5, 2), authInfo = _useState6[0], setAuthInfo = _useState6[1];
                var _useState7 = (0, react.useState)(""), _useState8 = (0, slicedToArray.Z)(_useState7, 2), cluster = _useState8[0], setCluster = _useState8[1];
                var _useState9 = (0, react.useState)(false), _useState10 = (0, slicedToArray.Z)(_useState9, 2), isModalOpen = _useState10[0], setIsModalOpen = _useState10[1];
                var navigate = (0, react_router_dist.s0)();
                var location = (0, react_router_dist.TH)();
                var _useAuth = useAuth(), auth = _useAuth.auth, signIn = _useAuth.signIn;
                var from = ((_location$state = location.state) === null || _location$state === void 0 ? void 0 : (_location$state$from = _location$state.from) === null || _location$state$from === void 0 ? void 0 : _location$state$from.pathname) || "/history";
                var onSwitchChanged = function onSwitchChanged(checked) {
                    if (checked) {
                        loglevel_default().setLevel("debug", true);
                    } else {
                        loglevel_default().setLevel("info", true);
                    }
                };
                var onFinish = function onFinish(values) {
                    var defaults = {
                        user: "default",
                        password: "",
                        cluster: "",
                        engine: null,
                        debug: false
                    };
                    Object.keys(values).forEach((function(key) {
                        if (values[key] === undefined || values[key] === "") {
                            values[key] = defaults[key];
                        }
                    }));
                    values["ip"] = window.location.hostname;
                    values["port"] = window.location.port;
                    if (values["port"] === "") {
                        if (window.location.protocol === "https:") {
                            values["port"] = 443;
                        } else if (window.location.protocol === "http:") {
                            values["port"] = 80;
                        } else {
                            throw Error("Unknown http protocol " + window.location.protocol);
                        }
                    }
                    values["ip"] = "localhost";
                    values["port"] = "8123";
                    loglevel_default().info("auth:", values);
                    canConnect(values).then((function(ok) {
                        if (ok) {
                            tryLogin(values).then((function(engine) {
                                engine.execute("name as cluster", "system.virtual_warehouses").then((function(rows) {
                                    var cluster_names = rows.map((function(row) {
                                        return row.cluster;
                                    }));
                                    loglevel_default().info("cluster:", cluster_names);
                                    var cluster_list = Array.from(new Set(cluster_names)).map((function(_cluster) {
                                        return {
                                            label: "".concat(_cluster),
                                            key: _cluster,
                                            icon: (0, jsx_runtime.jsx)(RightOutlined.Z, {})
                                        };
                                    }));
                                    cluster_list.push({
                                        label: "None (i.e., from local shard)",
                                        key: "",
                                        icon: (0, jsx_runtime.jsx)(RightOutlined.Z, {})
                                    });
                                    setAuthInfo(values);
                                    setClusters(cluster_list);
                                    setIsModalOpen(true);
                                }));
                            })).catch((function(err) {
                                setVerifying(false);
                                message.ZP.error("Wrong username/password");
                            }));
                        } else {
                            message.ZP.error("Cannot connect to the server; please make sure the server is up and accessible (e.g., via port-forward)");
                        }
                    })).catch((function(error) {
                        loglevel_default().error(error);
                        message.ZP.error(error.message);
                    }));
                };
                (0, react.useEffect)((function() {
                    if ((!auth || !auth.loggedIn) && instruction !== "noautologin") {
                        setVerifying(true);
                        localforage_default().getItem(AuthKey).then((function(ret) {
                            return signIn(ret, (function(_) {
                                loglevel_default().info("login via cache auth:", ret);
                                setVerifying(false);
                                navigate(from, {
                                    replace: true
                                });
                            })).catch((function(err) {
                                setVerifying(false);
                                loglevel_default().error(err.message);
                            }));
                        })).catch((function(err) {
                            setVerifying(false);
                            loglevel_default().error(err.message);
                        }));
                    }
                }), []);
                var handleClusterSelected = function handleClusterSelected() {
                    setIsModalOpen(false);
                    loglevel_default().debug("cache the auth: ", authInfo, " cluster: ", cluster);
                    localforage_default().setItem(AuthKey, (0, objectSpread2.Z)((0, objectSpread2.Z)({}, authInfo), {}, {
                        cluster: cluster,
                        loggedIn: false
                    })).then((function(_) {
                        navigate("/history", {
                            replace: true
                        });
                    }));
                };
                return (0, jsx_runtime.jsxs)(jsx_runtime.Fragment, {
                    children: [ (0, jsx_runtime.jsxs)(modal.Z, {
                        open: isModalOpen,
                        title: null,
                        onOk: handleClusterSelected,
                        closeIcon: null,
                        closable: false,
                        cancelButtonProps: {
                            disabled: true
                        },
                        children: [ (0, jsx_runtime.jsxs)("span", {
                            children: [ "Get the System Tables from ", (0, jsx_runtime.jsx)("strong", {
                                children: "Virtual Warehouse"
                            }), ":" ]
                        }), (0, jsx_runtime.jsx)(dropdown.Z, {
                            menu: {
                                items: clusters,
                                onClick: function onClick(e) {
                                    return setCluster(e.key);
                                }
                            },
                            children: (0, jsx_runtime.jsx)(es_button.Z, {
                                type: "primary",
                                block: true,
                                children: (0, jsx_runtime.jsxs)(space.Z, {
                                    children: [ cluster ? cluster : "None (i.e., local shard)", (0, jsx_runtime.jsx)(DownOutlined.Z, {}) ]
                                })
                            })
                        }) ]
                    }), (0, jsx_runtime.jsx)("section", {
                        style: {
                            marginTop: 20
                        },
                        children: (0, jsx_runtime.jsx)("div", {
                            className: "columns",
                            children: (0, jsx_runtime.jsx)("div", {
                                className: "column is-6 is-offset-3",
                                children: (0, jsx_runtime.jsx)(spin.Z, {
                                    spinning: verifying,
                                    children: (0, jsx_runtime.jsxs)(es_form.Z, {
                                        name: "normal_login",
                                        className: "login-form",
                                        initialValues: {
                                            remember: true
                                        },
                                        onFinish: onFinish,
                                        labelCol: {
                                            span: 6,
                                            offset: 0
                                        },
                                        wrapperCol: {
                                            span: 16,
                                            offset: 0
                                        },
                                        children: [ (0, jsx_runtime.jsx)(es_form.Z.Item, {
                                            name: "user",
                                            label: "Username",
                                            tooltip: "If not given, 'default' is used",
                                            children: (0, jsx_runtime.jsx)(input.Z, {
                                                prefix: (0, jsx_runtime.jsx)(UserOutlined.Z, {
                                                    className: "site-form-item-icon"
                                                }),
                                                placeholder: "default"
                                            })
                                        }), (0, jsx_runtime.jsx)(es_form.Z.Item, {
                                            name: "password",
                                            label: "Password",
                                            tooltip: "If not given, '' (empty string) is used",
                                            children: (0, jsx_runtime.jsx)(input.Z, {
                                                prefix: (0, jsx_runtime.jsx)(LockOutlined.Z, {
                                                    className: "site-form-item-icon"
                                                }),
                                                type: "password",
                                                placeholder: ""
                                            })
                                        }), (0, jsx_runtime.jsxs)(es_form.Z.Item, {
                                            wrapperCol: {
                                                span: 16,
                                                offset: 6
                                            },
                                            children: [ (0, jsx_runtime.jsx)("span", {
                                                children: "Debug Mode:"
                                            }), (0, jsx_runtime.jsx)(es_switch.Z, {
                                                defaultChecked: false,
                                                onChange: onSwitchChanged
                                            }), (0, jsx_runtime.jsx)(es_button.Z, {
                                                type: "primary",
                                                htmlType: "submit",
                                                style: {
                                                    position: "absolute",
                                                    right: 0,
                                                    width: "30%"
                                                },
                                                children: "Log in"
                                            }) ]
                                        }) ]
                                    })
                                })
                            })
                        })
                    }), (0, jsx_runtime.jsx)("section", {
                        children: (0, jsx_runtime.jsx)("div", {
                            className: "columns",
                            children: (0, jsx_runtime.jsx)("div", {
                                className: "column is-offset-3 is-6",
                                children: (0, jsx_runtime.jsxs)("div", {
                                    className: "content",
                                    children: [ (0, jsx_runtime.jsxs)("ol", {
                                        children: [ (0, jsx_runtime.jsx)(Title, {
                                            level: 4,
                                            children: "Server Configuration and Setting"
                                        }), (0, jsx_runtime.jsxs)("li", {
                                            children: [ "Set up", " ", (0, jsx_runtime.jsx)(Link, {
                                                href: "https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings#query-log",
                                                target: "_blank",
                                                rel: "noopener noreferrer",
                                                children: "system.query_log"
                                            }), " ", "and ", (0, jsx_runtime.jsx)(Text, {
                                                code: true,
                                                children: "set log_queries=1"
                                            }), " ", "for the basic features." ]
                                        }), (0, jsx_runtime.jsxs)("li", {
                                            children: [ "Set up ", (0, jsx_runtime.jsx)(Text, {
                                                code: true,
                                                children: "system.processors_profile_log"
                                            }), " ", "similarly as ", (0, jsx_runtime.jsx)(Text, {
                                                code: true,
                                                children: "system.query_log"
                                            }), "and ", (0, jsx_runtime.jsx)(Text, {
                                                code: true,
                                                children: "set log_processors_profiles=1"
                                            }), " ", "for displaying processor level runtime metrics in the processor graph." ]
                                        }), (0, jsx_runtime.jsxs)("li", {
                                            children: [ "Combine ", (0, jsx_runtime.jsx)(Text, {
                                                code: true,
                                                children: "print_graphviz=1"
                                            }), " with", " ", (0, jsx_runtime.jsx)(Text, {
                                                code: true,
                                                children: "print_graphviz_ast"
                                            }), " or", " ", (0, jsx_runtime.jsx)(Text, {
                                                code: true,
                                                children: "print_graphviz_plan"
                                            }), " or", " ", (0, jsx_runtime.jsx)(Text, {
                                                code: true,
                                                children: "print_graphviz_pipeline"
                                            }), " ", "for displaying logical plans generated by the query optimizer." ]
                                        }), (0, jsx_runtime.jsxs)("li", {
                                            children: [ "Set up", " ", (0, jsx_runtime.jsx)(Link, {
                                                href: "https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings#trace-log",
                                                target: "_blank",
                                                rel: "noopener noreferrer",
                                                children: "system.trace_log"
                                            }), " ", "and set ", (0, jsx_runtime.jsx)(Text, {
                                                code: true,
                                                children: "query_profiler_cpu_time_period_ns"
                                            }), " ", ", e.g., 10000000 (sampling 100 times per second) for CPU FlameGraph." ]
                                        }) ]
                                    }), (0, jsx_runtime.jsxs)("ol", {
                                        children: [ (0, jsx_runtime.jsx)(Title, {
                                            level: 4,
                                            children: "FAQ"
                                        }), (0, jsx_runtime.jsxs)("li", {
                                            children: [ "Q: It reports wrong username/password, but my username and password are correct.", (0, 
                                            jsx_runtime.jsx)("br", {}), "A: Try open the website using Chrome's Icognito Window." ]
                                        }) ]
                                    }) ]
                                })
                            })
                        })
                    }) ]
                });
            }
            function Layout() {
                var navigate = (0, react_router_dist.s0)();
                var _useAuth = useAuth(), auth = _useAuth.auth, signOut = _useAuth.signOut;
                var logout = function logout(event) {
                    event.preventDefault();
                    signOut((function() {
                        return navigate("/login/noautologin");
                    }));
                };
                return (0, jsx_runtime.jsxs)(jsx_runtime.Fragment, {
                    children: [ (0, jsx_runtime.jsxs)("nav", {
                        className: "navbar is-primary",
                        role: "navigation",
                        "aria-label": "main navigation",
                        children: [ (0, jsx_runtime.jsx)("div", {
                            className: "navbar-brand",
                            children: (0, jsx_runtime.jsx)("div", {
                                className: "navbar-item",
                                children: (0, jsx_runtime.jsx)("img", {
                                    src: "./byt-logo-128.png",
                                    alt: "logo img is missing"
                                })
                            })
                        }), (0, jsx_runtime.jsxs)("div", {
                            id: "navbarBasicExample",
                            className: "navbar-menu",
                            children: [ (0, jsx_runtime.jsxs)("div", {
                                className: "navbar-start",
                                children: [ (0, jsx_runtime.jsx)(dist.OL, {
                                    className: function className(_ref) {
                                        var isActive = _ref.isActive;
                                        return isActive ? "navbar-item is-active" : "navbar-item";
                                    },
                                    to: "/history",
                                    children: "History"
                                }), (0, jsx_runtime.jsx)(dist.OL, {
                                    className: function className(_ref2) {
                                        var isActive = _ref2.isActive;
                                        return isActive ? "navbar-item is-active" : "navbar-item";
                                    },
                                    to: "/starrocks",
                                    children: "StarRocks"
                                }) ]
                            }), (0, jsx_runtime.jsx)("div", {
                                className: "navbar-end",
                                children: auth && auth.loggedIn ? (0, jsx_runtime.jsxs)(jsx_runtime.Fragment, {
                                    children: [ (0, jsx_runtime.jsx)(dist.rU, {
                                        className: "navbar-item",
                                        to: "/",
                                        onClick: logout,
                                        children: "Logout"
                                    }), (0, jsx_runtime.jsxs)("div", {
                                        className: "navbar-item has-dropdown is-hoverable",
                                        children: [ (0, jsx_runtime.jsxs)(dist.rU, {
                                            className: "navbar-link",
                                            to: "/system/version",
                                            children: [ " ", auth.engine.scmVersion || auth.engine.fullVersion, " " ]
                                        }), (0, jsx_runtime.jsxs)("div", {
                                            className: "navbar-dropdown",
                                            children: [ (0, jsx_runtime.jsxs)(dist.rU, {
                                                className: "navbar-item",
                                                to: "/system/version",
                                                children: [ " ", "Version" ]
                                            }, "version"), (0, jsx_runtime.jsxs)(dist.rU, {
                                                className: "navbar-item",
                                                to: "/system/setting",
                                                children: [ " ", "Setting" ]
                                            }, "setting") ]
                                        }) ]
                                    }) ]
                                }) : (0, jsx_runtime.jsx)(dist.OL, {
                                    className: function className(_ref3) {
                                        var isActive = _ref3.isActive;
                                        return isActive ? "navbar-item is-active" : "navbar-item";
                                    },
                                    to: "/login",
                                    children: "Login"
                                })
                            }) ]
                        }) ]
                    }), (0, jsx_runtime.jsx)(react_router_dist.j3, {}) ]
                });
            }
            var date_picker = __webpack_require__(1666);
            var toConsumableArray = __webpack_require__(3433);
            var regeneratorRuntime = __webpack_require__(4165);
            var asyncToGenerator = __webpack_require__(5861);
            var defineProperty = __webpack_require__(4942);
            var objectWithoutProperties = __webpack_require__(4925);
            var _excluded = [ "index" ], _excluded2 = [ "title", "editable", "children", "dataIndex", "record", "handleSave" ];
            function TableWithEditableRemarks(_ref) {
                var table = _ref.table, rowSelection = _ref.rowSelection;
                var _useState = (0, react.useState)(table.dataSource), _useState2 = (0, slicedToArray.Z)(_useState, 2), dataSource = _useState2[0], setDataSource = _useState2[1];
                var RemarksPrefix = "@#";
                var EditableContext = (0, react.createContext)(null);
                var EditableRow = function EditableRow(_ref2) {
                    var index = _ref2.index, props = (0, objectWithoutProperties.Z)(_ref2, _excluded);
                    var _Form$useForm = es_form.Z.useForm(), _Form$useForm2 = (0, slicedToArray.Z)(_Form$useForm, 1), form = _Form$useForm2[0];
                    return (0, jsx_runtime.jsx)(es_form.Z, {
                        form: form,
                        component: false,
                        children: (0, jsx_runtime.jsx)(EditableContext.Provider, {
                            value: form,
                            children: (0, jsx_runtime.jsx)("tr", (0, objectSpread2.Z)({}, props))
                        })
                    });
                };
                var EditableCell = function EditableCell(_ref3) {
                    var title = _ref3.title, editable = _ref3.editable, children = _ref3.children, dataIndex = _ref3.dataIndex, record = _ref3.record, handleSave = _ref3.handleSave, restProps = (0, 
                    objectWithoutProperties.Z)(_ref3, _excluded2);
                    var _useState3 = (0, react.useState)(false), _useState4 = (0, slicedToArray.Z)(_useState3, 2), editing = _useState4[0], setEditing = _useState4[1];
                    var inputRef = (0, react.useRef)(null);
                    var form = (0, react.useContext)(EditableContext);
                    (0, react.useEffect)((function() {
                        if (editing) {
                            inputRef.current.focus();
                        }
                    }), [ editing ]);
                    var toggleEdit = function toggleEdit() {
                        setEditing(!editing);
                        form.setFieldsValue((0, defineProperty.Z)({}, dataIndex, record[dataIndex]));
                    };
                    var save = function() {
                        var _ref4 = (0, asyncToGenerator.Z)((0, regeneratorRuntime.Z)().mark((function _callee() {
                            var values;
                            return (0, regeneratorRuntime.Z)().wrap((function _callee$(_context) {
                                while (1) {
                                    switch (_context.prev = _context.next) {
                                      case 0:
                                        _context.prev = 0;
                                        _context.next = 3;
                                        return form.validateFields();

                                      case 3:
                                        values = _context.sent;
                                        loglevel_default().debug("saved: ", values);
                                        toggleEdit();
                                        handleSave((0, objectSpread2.Z)((0, objectSpread2.Z)({}, record), values));
                                        _context.next = 12;
                                        break;

                                      case 9:
                                        _context.prev = 9;
                                        _context.t0 = _context["catch"](0);
                                        loglevel_default().error("Save failed:", _context.t0);

                                      case 12:
                                      case "end":
                                        return _context.stop();
                                    }
                                }
                            }), _callee, null, [ [ 0, 9 ] ]);
                        })));
                        return function save() {
                            return _ref4.apply(this, arguments);
                        };
                    }();
                    var childNode = children;
                    if (editable) {
                        childNode = editing ? (0, jsx_runtime.jsx)(es_form.Z.Item, {
                            style: {
                                margin: 0
                            },
                            name: dataIndex,
                            rules: [ {
                                required: true,
                                message: "".concat(title, " is required.")
                            } ],
                            children: (0, jsx_runtime.jsx)(input.Z, {
                                ref: inputRef,
                                onPressEnter: save,
                                onBlur: save
                            })
                        }) : (0, jsx_runtime.jsx)("div", {
                            className: "editable-cell-value-wrap",
                            onClick: toggleEdit,
                            children: children
                        });
                    }
                    return (0, jsx_runtime.jsx)("td", (0, objectSpread2.Z)((0, objectSpread2.Z)({}, restProps), {}, {
                        children: childNode
                    }));
                };
                var handleSave = function handleSave(row) {
                    var newData = (0, toConsumableArray.Z)(dataSource);
                    var index = newData.findIndex((function(item) {
                        return row.key === item.key;
                    }));
                    var item = newData[index];
                    newData.splice(index, 1, (0, objectSpread2.Z)((0, objectSpread2.Z)({}, item), row));
                    setDataSource(newData);
                    loglevel_default().debug("save remarks", row);
                    localforage_default().setItem(RemarksPrefix + row.initial_query_id, row.remarks);
                };
                var components = {
                    body: {
                        row: EditableRow,
                        cell: EditableCell
                    }
                };
                var editableColumns = [].concat((0, toConsumableArray.Z)(table.columns), [ {
                    title: "Remarks",
                    dataIndex: "remarks",
                    editable: true
                } ]).map((function(col) {
                    if (!col.editable) {
                        return col;
                    }
                    return (0, objectSpread2.Z)((0, objectSpread2.Z)({}, col), {}, {
                        onCell: function onCell(record) {
                            return {
                                record: record,
                                editable: col.editable,
                                dataIndex: col.dataIndex,
                                title: col.title,
                                handleSave: handleSave
                            };
                        }
                    });
                }));
                (0, react.useEffect)((function() {
                    var rows = table.dataSource.slice();
                    Promise.all(rows.map((function(row) {
                        return localforage_default().getItem(RemarksPrefix + row.initial_query_id);
                    }))).then((function(all_remarks) {
                        all_remarks.forEach((function(remarks, idx) {
                            return rows[idx].remarks = remarks || "null";
                        }));
                        setDataSource(rows);
                    }));
                }), [ table ]);
                return (0, jsx_runtime.jsx)("div", {
                    children: (0, jsx_runtime.jsx)(es_table.Z, {
                        components: components,
                        dataSource: dataSource,
                        columns: editableColumns,
                        size: "small",
                        scroll: {
                            y: 600
                        },
                        pagination: false,
                        rowSelection: rowSelection
                    })
                });
            }
            var RangePicker = date_picker.Z.RangePicker;
            function getRecentQueries(startDateTime, endDateTime, includeSysQueries, engine) {
                var topn = arguments.length > 4 && arguments[4] !== undefined ? arguments[4] : 200;
                var where = "";
                if (startDateTime) {
                    var dateTimeStr = startDateTime.format("YYYY-MM-DD HH:mm:ss");
                    where += "event_date >= ".concat(engine.toServerDate(dateTimeStr), " and event_time >= ").concat(engine.toServerDateTime(dateTimeStr), " and ");
                }
                if (endDateTime) {
                    var _dateTimeStr = endDateTime.format("YYYY-MM-DD HH:mm:ss");
                    where += "event_date <= ".concat(engine.toServerDate(_dateTimeStr), " and event_time <= ").concat(engine.toServerDateTime(_dateTimeStr), " and ");
                }
                where += "type != 1 and is_initial_query = 1 and query_duration_ms > 10 ";
                if (!includeSysQueries) {
                    where += " and " + engine.getExcludeSystemTablePredicate();
                }
                where += "order by event_time DESC LIMIT ".concat(topn);
                return engine.execute("initial_query_id, event_date, ".concat(engine.toLocalTZ("query_start_time_microseconds", "start"), ", query_duration_ms, read_rows, written_rows"), "system.query_log", where);
            }
            function getSystemFlameGraphURL(startDateTime, endDateTime, engine) {
                startDateTime.seconds(0);
                endDateTime.seconds(0);
                var where = "";
                var dateTimeStr = startDateTime.format("YYYY-MM-DD HH:mm:ss");
                where += "event_date >= ".concat(engine.toServerDate(dateTimeStr), " and event_time >= ").concat(engine.toServerDateTime(dateTimeStr), " and ");
                dateTimeStr = endDateTime.format("YYYY-MM-DD HH:mm:ss");
                where += "event_date <= ".concat(engine.toServerDate(dateTimeStr), " and event_time <= ").concat(engine.toServerDateTime(dateTimeStr));
                return engine.getFlameGraphURL(where);
            }
            function History() {
                var _useState = (0, react.useState)({
                    columns: [],
                    dataSource: []
                }), _useState2 = (0, slicedToArray.Z)(_useState, 2), table = _useState2[0], setTable = _useState2[1];
                var _useState3 = (0, react.useState)(), _useState4 = (0, slicedToArray.Z)(_useState3, 2), startTime = _useState4[0], setStartTime = _useState4[1];
                var _useState5 = (0, react.useState)(), _useState6 = (0, slicedToArray.Z)(_useState5, 2), endTime = _useState6[0], setEndTime = _useState6[1];
                var _useState7 = (0, react.useState)(null), _useState8 = (0, slicedToArray.Z)(_useState7, 2), profileURL = _useState8[0], setProfileURL = _useState8[1];
                var _useState9 = (0, react.useState)(false), _useState10 = (0, slicedToArray.Z)(_useState9, 2), sysQueries = _useState10[0], setSysQueries = _useState10[1];
                var _useState11 = (0, react.useState)(true), _useState12 = (0, slicedToArray.Z)(_useState11, 2), isLoading = _useState12[0], setIsLoading = _useState12[1];
                var _useAuth = useAuth(), auth = _useAuth.auth;
                var onDateChange = function onDateChange(value) {
                    setIsLoading(true);
                    setStartTime(value[0]);
                    setEndTime(value[1]);
                };
                var deps = [ startTime, endTime, sysQueries ];
                (0, react.useEffect)((function() {
                    if (auth && auth.loggedIn) {
                        var start = startTime;
                        var end = endTime;
                        var QueryInfo = [ [ "initial_query_id", "Query ID", "", "" ], [ "start", "Start Time", "", "Date" ], [ "query_duration_ms", "Duration (ms)", "", "Int" ], [ "read_rows", "Read Rows", "Total number of rows read from all tables and table functions across all replicas excluding cache", "Int" ], [ "written_rows", "Written Rows", "Total number of rows written by insert queries", "Int" ] ];
                        getRecentQueries(start, end, sysQueries, auth.engine).then((function(rows) {
                            setIsLoading(false);
                            if (rows.length === 0) {
                                var errorMsgKey = "historyErrorMsgKey";
                                message.ZP.warning({
                                    content: "There are no queries in the selected date range. Please check 1. the date range; " + "2. log_queries=1; 3. query_log in configuration xml/yaml file; 4. Show System Queries switch",
                                    duration: 5,
                                    key: errorMsgKey,
                                    onClick: function onClick() {
                                        return message.ZP.destroy(errorMsgKey);
                                    }
                                });
                                return;
                            }
                            var _prepareTable = prepareTable(QueryInfo, rows), colDefs = _prepareTable.columns, data = _prepareTable.dataSource;
                            colDefs[0].render = function(_, record) {
                                return (0, jsx_runtime.jsx)(dist.rU, {
                                    to: "query/".concat(record.initial_query_id, "+").concat(record.event_date),
                                    children: record.initial_query_id
                                });
                            };
                            colDefs[0].width = 400;
                            colDefs[1].width = 250;
                            setTable({
                                columns: colDefs,
                                dataSource: data
                            });
                        })).catch((function(err) {
                            loglevel_default().error(err);
                            message.ZP.error("Check console for the error message; You need to set up the query_log (ref CK community website) in server.xml if the error is due to table missing.");
                        }));
                        if (start && end) {
                            try {
                                getSystemFlameGraphURL(start, end, auth.engine).then((function(pURL) {
                                    return setProfileURL(pURL);
                                }));
                            } catch (err) {
                                loglevel_default().warn(err);
                                setProfileURL(null);
                            }
                        } else {
                            setProfileURL(null);
                        }
                    } else {
                        message.ZP.error("Please go to Login page to connect the engine");
                    }
                }), deps);
                var refresh = function refresh() {
                    setIsLoading(true);
                    setEndTime(moment_default()());
                    setStartTime(null);
                };
                var toggleSystemQueries = function toggleSystemQueries() {
                    setIsLoading(true);
                    setSysQueries(!sysQueries);
                };
                return (0, jsx_runtime.jsxs)("section", {
                    children: [ (0, jsx_runtime.jsx)("div", {
                        className: "box",
                        children: (0, jsx_runtime.jsx)("div", {
                            className: "level",
                            children: (0, jsx_runtime.jsxs)("div", {
                                className: "level-left",
                                children: [ (0, jsx_runtime.jsx)("div", {
                                    className: "level-item",
                                    children: (0, jsx_runtime.jsx)("span", {
                                        children: "Query DateTime"
                                    })
                                }), (0, jsx_runtime.jsx)("div", {
                                    className: "level-item",
                                    children: (0, jsx_runtime.jsx)(RangePicker, {
                                        size: "large",
                                        showTime: {
                                            format: "HH:mm"
                                        },
                                        value: [ startTime, endTime ],
                                        format: "YYYY-MM-DD HH:mm",
                                        onChange: onDateChange
                                    })
                                }), (0, jsx_runtime.jsx)("div", {
                                    className: "level-item",
                                    children: (0, jsx_runtime.jsx)(tooltip.Z, {
                                        title: "Get the latest 200 queries",
                                        children: (0, jsx_runtime.jsx)("button", {
                                            className: "button is-link is-inverted",
                                            onClick: refresh,
                                            children: "Refresh"
                                        })
                                    })
                                }), (0, jsx_runtime.jsx)("div", {
                                    className: "level-item",
                                    children: (0, jsx_runtime.jsxs)("label", {
                                        className: "checkbox",
                                        children: [ (0, jsx_runtime.jsx)("input", {
                                            type: "checkbox",
                                            onClick: toggleSystemQueries
                                        }), "Show System Queries" ]
                                    })
                                }), (0, jsx_runtime.jsx)("div", {
                                    className: "level-item",
                                    children: (0, jsx_runtime.jsx)(tooltip.Z, {
                                        title: "To show the perf flamegraph of the servers during [Start, End]. " + (profileURL ? "Please avoid a big time window which may lead to timeout due to too many result records to transfer" : auth.engine.name === "Vanilla" ? "Not available for Vanilla engine" : "Select the Start and End time and set up trace_log in server config file " + "following https://clickhouse.com/docs/en/operations/optimizing-performance/sampling-query-profiler/"),
                                        children: profileURL ? (0, jsx_runtime.jsx)("a", {
                                            target: "_blank",
                                            rel: "noopener noreferrer",
                                            href: "https://www.speedscope.app/#profileURL=".concat(profileURL),
                                            children: "FlameGraph"
                                        }) : (0, jsx_runtime.jsx)("a", {
                                            style: {
                                                color: "#959595"
                                            },
                                            onClick: function onClick(e) {
                                                return e.preventDefault();
                                            },
                                            href: "https://www.speedscope.app/",
                                            children: "FlameGraph"
                                        })
                                    })
                                }) ]
                            })
                        })
                    }), (0, jsx_runtime.jsx)("div", {
                        className: "box",
                        children: (0, jsx_runtime.jsx)(spin.Z, {
                            spinning: isLoading,
                            children: (0, jsx_runtime.jsx)(TableWithEditableRemarks, {
                                table: table
                            })
                        })
                    }) ]
                });
            }
            var menu = __webpack_require__(7890);
            function getQueryFlameGraphURL(qid, date, engine) {
                return engine.getFlameGraphURL("event_date = toDate('".concat(date, "') and startsWith(query_id, '").concat(qid, "')"));
            }
            function Query() {
                var _useState = (0, react.useState)("overview"), _useState2 = (0, slicedToArray.Z)(_useState, 2), current = _useState2[0], setCurrent = _useState2[1];
                var _useState3 = (0, react.useState)(false), _useState4 = (0, slicedToArray.Z)(_useState3, 2), loading = _useState4[0], setLoading = _useState4[1];
                var _useState5 = (0, react.useState)([]), _useState6 = (0, slicedToArray.Z)(_useState5, 2), profileItem = _useState6[0], setProfileItem = _useState6[1];
                var _useParams = (0, react_router_dist.UO)(), qid_date = _useParams.qid_date;
                var _useAuth = useAuth(), auth = _useAuth.auth;
                var onClick = function onClick(e) {
                    setCurrent(e.key);
                };
                var items = [ {
                    label: (0, jsx_runtime.jsx)(dist.OL, {
                        to: "/query/".concat(qid_date),
                        className: function className(_ref) {
                            var isActive = _ref.isActive;
                            return isActive ? "navbar-item is-active" : "navbar-item";
                        },
                        children: "Overview"
                    }),
                    key: "overview"
                }, {
                    label: (0, jsx_runtime.jsx)(dist.OL, {
                        to: "/query/".concat(qid_date, "/optimizer/"),
                        className: function className(_ref2) {
                            var isActive = _ref2.isActive;
                            return isActive ? "navbar-item is-active" : "navbar-item";
                        },
                        children: "Optimizer"
                    }),
                    key: "optimizer"
                }, {
                    label: (0, jsx_runtime.jsx)(dist.OL, {
                        to: "/query/".concat(qid_date, "/processor/"),
                        className: function className(_ref3) {
                            var isActive = _ref3.isActive;
                            return isActive ? "navbar-item is-active" : "navbar-item";
                        },
                        children: "Processor"
                    }),
                    key: "processor"
                }, {
                    label: (0, jsx_runtime.jsx)(dist.OL, {
                        to: "/query/".concat(qid_date, "/profile_events/"),
                        className: function className(_ref4) {
                            var isActive = _ref4.isActive;
                            return isActive ? "navbar-item is-active" : "navbar-item";
                        },
                        children: "ProfileEvents"
                    }),
                    key: "profile_events"
                } ];
                (0, react.useEffect)((function() {
                    var _qid_date$split = qid_date.split("+"), _qid_date$split2 = (0, slicedToArray.Z)(_qid_date$split, 2), qid = _qid_date$split2[0], date = _qid_date$split2[1];
                    if (auth && auth.loggedIn) {
                        getQueryFlameGraphURL(qid, date, auth.engine).then((function(profileURL) {
                            setProfileItem([ {
                                label: (0, jsx_runtime.jsx)(tooltip.Z, {
                                    title: "View flamegraph of this query on speedscope website. https://clickhouse.com/docs/en/operations/optimizing-performance/sampling-query-profiler/",
                                    children: (0, jsx_runtime.jsx)("a", {
                                        target: "_blank",
                                        rel: "noopener noreferrer",
                                        href: "https://www.speedscope.app/#profileURL=".concat(profileURL),
                                        className: "navbar-item",
                                        children: "FlameGraph"
                                    })
                                }),
                                key: "flamegraph"
                            } ]);
                            setLoading(false);
                        })).catch((function(err) {
                            loglevel_default().error(err.message);
                            setProfileItem([ {
                                label: (0, jsx_runtime.jsx)(tooltip.Z, {
                                    title: "No cpu trace records for this query. Set up trace_log in server config file and set the cpu sampling interval/frequency https://clickhouse.com/docs/en/operations/optimizing-performance/sampling-query-profiler/",
                                    children: (0, jsx_runtime.jsx)("a", {
                                        style: {
                                            color: "#959595"
                                        },
                                        onClick: function onClick(e) {
                                            return e.preventDefault();
                                        },
                                        href: "https://www.speedscope.app/",
                                        className: "navbar-item",
                                        children: "FlameGraph"
                                    })
                                }),
                                key: "flamegraph"
                            } ]);
                        }));
                    }
                }), [ qid_date, auth ]);
                return (0, jsx_runtime.jsxs)(jsx_runtime.Fragment, {
                    children: [ (0, jsx_runtime.jsx)(spin.Z, {
                        spinning: loading,
                        children: (0, jsx_runtime.jsx)(menu.Z, {
                            onClick: onClick,
                            selectedKeys: [ current ],
                            items: [].concat(items, (0, toConsumableArray.Z)(profileItem)),
                            mode: "horizontal"
                        })
                    }), " ", (0, jsx_runtime.jsx)(react_router_dist.j3, {}) ]
                });
            }
            function setProcessorLevelByBFS(root_processors_ids, id2processor, children) {
                var cur_processors_ids = root_processors_ids;
                var visited = new Set;
                var level = 0;
                var _loop = function _loop() {
                    var next_processors_ids = [];
                    cur_processors_ids.forEach((function(pid) {
                        if (visited.has(pid)) {
                            return;
                        }
                        id2processor.get(pid).level = level;
                        visited.add(pid);
                        if (children.has(pid)) {
                            children.get(pid).forEach((function(cid) {
                                next_processors_ids.push(cid);
                            }));
                        }
                    }));
                    cur_processors_ids = next_processors_ids;
                    level += 1;
                };
                while (cur_processors_ids.length) {
                    _loop();
                }
            }
            function getGroupName(processor) {
                return "".concat(processor.level, "-").concat(processor.name);
            }
            function parseGroupName(group_name) {
                var pos1 = group_name.indexOf("-");
                if (pos1 > 0) {
                    var level = (0, lodash.parseInt)(group_name.slice(0, pos1));
                    var name = group_name.slice(pos1 + 1);
                    return {
                        level: level,
                        name: name
                    };
                }
                throw Error("Expected group name like <level>-<procesor_name>; but got ".concat(group_name));
            }
            function groupProcessors(processors) {
                var groups = new Map;
                processors.forEach((function(processor) {
                    var group_name = getGroupName(processor);
                    if (groups.has(group_name)) {
                        groups.get(group_name).members.push(processor);
                    } else {
                        groups.set(group_name, {
                            members: [ processor ]
                        });
                    }
                }));
                return groups;
            }
            function connectGroups(groups, id2processor, id2child_processor) {
                groups.forEach((function(group) {
                    group.children = [];
                    var children_name_set = new Set;
                    group.members.forEach((function(processor) {
                        if (id2child_processor.has(processor.id)) {
                            id2child_processor.get(processor.id).forEach((function(child) {
                                var child_group_name = getGroupName(id2processor.get(child));
                                if (!children_name_set.has(child_group_name)) {
                                    group.children.push(groups.get(child_group_name));
                                    children_name_set.add(child_group_name);
                                }
                            }));
                        }
                    }));
                }));
            }
            function groupProcessorsInSegment(processors) {
                var id2processor = new Map;
                var children = new Map;
                var root_processors_ids = [];
                processors.forEach((function(processor) {
                    id2processor.set(processor.id, processor);
                    if (processor.parent_ids.length === 0) {
                        root_processors_ids.push(processor.id);
                    }
                    processor.parent_ids.forEach((function(pid) {
                        if (!children.has(pid)) {
                            children.set(pid, new Set);
                        }
                        children.get(pid).add(processor.id);
                    }));
                }));
                setProcessorLevelByBFS(root_processors_ids, id2processor, children);
                var groups = groupProcessors(processors);
                connectGroups(groups, id2processor, children);
                return groups;
            }
            function renameExchangeSource(name) {
                var fs = name.split("-");
                if (fs.length > 2) {
                    var pos = fs[fs.length - 1].indexOf("_");
                    if (pos > -1) {
                        name = fs[fs.length - 1].slice(pos + 1);
                    } else {
                        throw Error("The ExchangeSource name ".concat(name, " contains query id, but no _ after it"));
                    }
                }
                var m = null;
                var exchange_id = -1;
                if ((m = name.match(/(\d+)_(\d+)_(\d+)_(\d+)?/)) !== null) {
                    var source_segment_id = (0, lodash.parseInt)(m[1]);
                    var target_segment_id = (0, lodash.parseInt)(m[2]);
                    if (m[4] !== undefined) {
                        exchange_id = (0, lodash.parseInt)(m[4]);
                    }
                    return "ExchangeSrc_".concat(source_segment_id, "_to_").concat(target_segment_id, "_").concat(exchange_id);
                } else {
                    throw Error("The processor name starts with ExchangeSource, but it does not have _SourceSegId_TargetSegId_ pattern:" + name);
                }
            }
            function parseExchangeSource(name) {
                var m = null;
                if ((m = name.match(/(\d+)_to_(\d+)_(\d+)/)) !== null) {
                    var source_segment_id = (0, lodash.parseInt)(m[1]);
                    var exchange_id = (0, lodash.parseInt)(m[3]);
                    return {
                        source_segment_id: source_segment_id,
                        exchange_id: exchange_id
                    };
                } else {
                    throw Error("ExchangeSrc_<source_segment_id>_to_<target_segment_id>_<exchange_id> pattern is expected; but received ".concat(name));
                }
            }
            function renameExchangeSink(name) {
                var end_pos = name.indexOf("]");
                if (end_pos !== -1) {
                    name = name.slice(0, end_pos + 1);
                }
                return name;
            }
            function parseExchangeSink(name) {
                var m = null;
                if ((m = name.match(/\[(\d+)\]/)) !== null) {
                    return (0, lodash.parseInt)(m[1]);
                } else {
                    return -1;
                }
            }
            function createGraphForSegment(segment_id, name2group) {
                var nodes = [];
                var edges = [];
                var inputs = [];
                var outputs = [];
                name2group.forEach((function(group, name) {
                    var node = getNodeName(segment_id, name);
                    nodes.push(node);
                    if (name.indexOf("ExchangeSrc") !== -1) {
                        var _parseExchangeSource = parseExchangeSource(name), source_segment_id = _parseExchangeSource.source_segment_id, exchange_id = _parseExchangeSource.exchange_id;
                        inputs.push({
                            node: node,
                            source_segment_id: source_segment_id,
                            exchange_id: exchange_id
                        });
                    } else if (name.indexOf("ExchangeSink") !== -1) {
                        var _exchange_id = parseExchangeSink(name);
                        outputs.push({
                            node: node,
                            exchange_id: _exchange_id
                        });
                    }
                    group.children.forEach((function(child_group) {
                        var child_node_name = getGroupName(child_group.members[0]);
                        edges.push({
                            src: getNodeName(segment_id, child_node_name),
                            dst: getNodeName(segment_id, name)
                        });
                    }));
                }));
                return {
                    nodes: nodes,
                    edges: edges,
                    inputs: inputs,
                    outputs: outputs
                };
            }
            function unionGraph(ga, gb) {
                var nodeSet = new Set(ga.nodes);
                var edgeSet = new Set(ga.edges.map((function(edge) {
                    return "".concat(edge.src, "->").concat(edge.dst);
                })));
                var inputSet = new Set(ga.inputs.map((function(input) {
                    return input.node;
                })));
                var outputSet = new Set(ga.outputs.map((function(output) {
                    return output.node;
                })));
                var nodes = (0, toConsumableArray.Z)(ga.nodes);
                var edges = (0, toConsumableArray.Z)(ga.edges);
                var inputs = (0, toConsumableArray.Z)(ga.inputs);
                var outputs = (0, toConsumableArray.Z)(ga.outputs);
                gb.nodes.forEach((function(node) {
                    if (!nodeSet.has(node)) {
                        nodeSet.add(node);
                        nodes.push(node);
                    }
                }));
                gb.edges.forEach((function(edge) {
                    var e = "".concat(edge.src, "->").concat(edge.dst);
                    if (!edgeSet.has(e)) {
                        edgeSet.add(e);
                        edges.push(edge);
                    }
                }));
                gb.inputs.forEach((function(b) {
                    if (!inputSet.has(b.node)) {
                        inputSet.add(b.node);
                        inputs.push(b);
                    }
                }));
                gb.outputs.forEach((function(b) {
                    if (!outputSet.has(b.node)) {
                        outputSet.add(b.node);
                        outputs.push(b);
                    }
                }));
                return {
                    nodes: nodes,
                    edges: edges,
                    inputs: inputs,
                    outputs: outputs
                };
            }
            function getGroupMetricAggregator(groups, segment, shard) {
                var aggregator = function aggregator(group_name, method, init_val, metric) {
                    var ret = init_val;
                    var method_func = method;
                    if (typeof method !== "function") {
                        if (method === "count") {
                            method_func = function method_func(processor, val) {
                                return val + 1;
                            };
                        } else if (method === "sum") {
                            method_func = function method_func(processor, val) {
                                return val + processor[metric];
                            };
                        } else if (method === "max") {
                            method_func = function method_func(processor, val) {
                                return Math.max(processor[metric], val);
                            };
                        } else if (method === "min") {
                            method_func = function method_func(processor, val) {
                                return Math.min(processor[metric], val);
                            };
                        } else {
                            throw Error("Unknown aggregation method " + method);
                        }
                    }
                    if (shard === "all") {
                        groups.get(segment).forEach((function(shard_groups) {
                            if (shard_groups.has(group_name)) {
                                shard_groups.get(group_name).members.forEach((function(processor) {
                                    return ret = method_func(processor, ret);
                                }));
                            } else {
                                loglevel_default().debug("in Segment ".concat(segment, ", ").concat(group_name, " is missing in a shard_groups"));
                            }
                        }));
                    } else {
                        var shard_groups = groups.get(segment).get((0, lodash.parseInt)(shard));
                        if (shard_groups === undefined) {
                            loglevel_default().debug("segment ".concat(segment, " has no info in shard ").concat(shard, "; the available shards are ").concat(Array.from(groups.get(segment).keys())));
                            return 0;
                        }
                        if (shard_groups.has(group_name)) {
                            shard_groups.get(group_name).members.forEach((function(processor) {
                                return ret = method_func(processor, ret);
                            }));
                        } else {
                            loglevel_default().debug("in Segment ".concat(segment, ", ").concat(group_name, " is missing in a shard_groups"));
                        }
                    }
                    return ret;
                };
                return aggregator;
            }
            function processor_toDotString(graphs, groups, segment, shard, color_exchange) {
                var dot = 'digraph { rankdir="BT" ';
                var convertForOneSegment = function convertForOneSegment(segment_id) {
                    var agg_func = getGroupMetricAggregator(groups, segment_id, shard);
                    var cluster_nodes = [];
                    graphs.get(segment_id).nodes.forEach((function(node_name) {
                        var _renderNodeBrief = renderNodeBrief(node_name, agg_func, color_exchange), label = _renderNodeBrief.label, fill = _renderNodeBrief.fill;
                        cluster_nodes.push('"'.concat(node_name, '" [id="').concat(node_name, '", label="').concat(label, '", shape=record, style="rounded, filled", fillcolor="').concat(fill, '"];'));
                    }));
                    var nodes_str = cluster_nodes.join("\n");
                    var cluster = "subgraph cluster_".concat(segment_id, ' \n { label="Segment ').concat(segment_id, '" style=dashed; fontsize=20; ').concat(nodes_str, "};");
                    var intra_edges = [];
                    graphs.get(segment_id).edges.forEach((function(_ref) {
                        var src = _ref.src, dst = _ref.dst;
                        var _parseNodeName = parseNodeName(src), src_name = _parseNodeName.group_name;
                        var output_rows = agg_func(src_name, "sum", 0, "output_rows");
                        intra_edges.push('"'.concat(src, '"->"').concat(dst, '"[label="').concat(convertUnit(output_rows), '"];'));
                    }));
                    var edges_str = intra_edges.join("\n");
                    return "".concat(cluster, " \n ").concat(edges_str);
                };
                if (segment !== "all") {
                    dot = dot + convertForOneSegment((0, lodash.parseInt)(segment));
                } else {
                    graphs.forEach((function(graph, segment_id) {
                        dot = dot + convertForOneSegment(segment_id);
                        var agg_func = getGroupMetricAggregator(groups, segment_id, shard);
                        var inter_edges = [];
                        graph.inputs.forEach((function(input_node) {
                            if (graphs.has(input_node.source_segment_id)) {
                                graphs.get(input_node.source_segment_id).outputs.forEach((function(output_node) {
                                    if (output_node.exchange_id === input_node.exchange_id) {
                                        var _parseNodeName2 = parseNodeName(input_node.node), group_name = _parseNodeName2.group_name;
                                        var output_rows = agg_func(group_name, "sum", 0, "output_rows");
                                        inter_edges.push('"'.concat(output_node.node, '"->"').concat(input_node.node, '"[label="').concat(convertUnit(output_rows), '"];'));
                                    }
                                }));
                            }
                        }));
                        dot = dot + "\n ".concat(inter_edges.join("\n"));
                    }));
                }
                return dot + "}";
            }
            function getNodeName(segment_id, name) {
                return "".concat(segment_id, "/").concat(name);
            }
            function parseNodeName(node_name) {
                var pos = node_name.indexOf("/");
                if (pos > 0) {
                    return {
                        segment_id: (0, lodash.parseInt)(node_name.slice(0, pos)),
                        group_name: node_name.slice(pos + 1)
                    };
                } else {
                    throw Error("Expected to receive node name in format like <semgent_id>/<group_name>; but got ".concat(node_name));
                }
            }
            function renderNodeBrief(node_name, agg_func, color_exchange) {
                var _parseNodeName3 = parseNodeName(node_name), group_name = _parseNodeName3.group_name;
                var _parseGroupName = parseGroupName(group_name), processor_name = _parseGroupName.name;
                var count = agg_func(group_name, "count", 0, "input_rows");
                var elapsed_us = agg_func(group_name, "max", 0, "elapsed_us");
                var fill_color = fillColor(processor_name, elapsed_us, color_exchange);
                var label_text = "{".concat(processor_name.slice(0, 25), "|{&#9201; ").concat(convertTimeUnit(elapsed_us), " | x").concat(count, "}}");
                return {
                    label: label_text,
                    fill: fill_color
                };
            }
            function prepareNodeDetails(node_name, shard, groups) {
                var _parseNodeName4 = parseNodeName(node_name), segment_id = _parseNodeName4.segment_id, group_name = _parseNodeName4.group_name;
                var _parseGroupName2 = parseGroupName(group_name), level = _parseGroupName2.level, processor_name = _parseGroupName2.name;
                var agg_func = getGroupMetricAggregator(groups, segment_id, shard);
                var input_rows = agg_func(group_name, "sum", 0, "input_rows");
                var output_rows = agg_func(group_name, "sum", 0, "output_rows");
                var call_times = agg_func(group_name, "sum", 0, "call_times");
                var rows = [];
                agg_func(group_name, (function(processor, val) {
                    val.push(processor);
                    return val;
                }), rows);
                var columns = [ [ "call_times", "Call Times", "", "Int" ], [ "input_rows", "Input Rows", "", "Int" ], [ "output_rows", "Output Rows", "", "Int" ], [ "elapsed_us", "Elapsed(us)", "", "Int" ], [ "input_wait_elapsed_us", "Input Wait(us)", "", "Int" ], [ "output_wait_elapsed_us", "Output wait(us)", "", "Int" ] ];
                return {
                    title: processor_name,
                    sections: [ {
                        header: "Basics",
                        fields: [ [ "Processor Name", processor_name ], [ "Segment ID", segment_id ], [ "Call Times", call_times ], [ "Input Rows", numberWithComma(input_rows) ], [ "Output Rows", numberWithComma(output_rows) ], [ "Group Level", level ] ]
                    }, {
                        header: "Plan Step of the Processor",
                        table: undefined,
                        scrollX: false
                    }, {
                        header: "Plan Steps of Other Processors in the Segment",
                        tables: undefined,
                        scrollX: false
                    }, {
                        header: "Processors",
                        table: prepareTable(columns, rows),
                        scrollX: true
                    } ]
                };
            }
            function processSegment(segment_id, segment_processors) {
                var segment_groups = new Map;
                var shards = new Set;
                var graph = undefined;
                segment_processors.forEach((function(shard_processors, shard) {
                    shards.add(shard);
                    var shard_groups = groupProcessorsInSegment(shard_processors);
                    segment_groups.set(shard, shard_groups);
                    if (graph === undefined) {
                        graph = createGraphForSegment(segment_id, shard_groups);
                    } else {
                        graph = unionGraph(graph, createGraphForSegment(segment_id, shard_groups));
                    }
                }));
                return {
                    segment_id: segment_id,
                    graph: graph,
                    segment_groups: segment_groups,
                    shards: shards
                };
            }
            function getDataFromEngine(qid, date, engine) {
                return engine.execute("bitAnd(bitShiftRight(plan_group, 16), 65535) as segment_id, \n         bitShiftRight(plan_group, 32) as call_times, \n         plan_step, id, name, parent_ids, elapsed_us, \n         input_rows, output_rows, input_wait_elapsed_us, output_wait_elapsed_us", "system.processors_profile_log", "startsWith(query_id,'".concat(qid, "') and event_date='").concat(date, "' ")).then((function(rows) {
                    if (rows.length === 0) {
                        var msg = "There is no processor log for qid=".concat(qid, ". Try: set log_proccessors_profiles=1; select the correct cluster name.");
                        message.ZP.error(msg);
                    }
                    var processors = new Map;
                    rows.forEach((function(row) {
                        if (row.name.startsWith("ExchangeSource")) {
                            row.name = renameExchangeSource(row.name);
                        }
                        if (row.name.indexOf("ExchangeSink") !== -1) {
                            row.name = renameExchangeSink(row.name);
                        }
                        if (!processors.has(row.segment_id)) {
                            processors.set(row.segment_id, new Map);
                        }
                        var segment_processors = processors.get(row.segment_id);
                        if (!segment_processors.has(row.shard)) {
                            segment_processors.set(row.shard, []);
                        }
                        segment_processors.get(row.shard).push(row);
                    }));
                    var all_groups = new Map;
                    var all_graphs = new Map;
                    var all_segments = new Set;
                    var all_shards = new Set;
                    processors.forEach((function(segment_processors, segment_id) {
                        var _processSegment = processSegment(segment_id, segment_processors), graph = _processSegment.graph, segment_groups = _processSegment.segment_groups, shards = _processSegment.shards;
                        all_groups.set(segment_id, segment_groups);
                        all_graphs.set(segment_id, graph);
                        all_segments.add(segment_id);
                        all_shards = new Set([].concat((0, toConsumableArray.Z)(all_shards), (0, toConsumableArray.Z)(shards)));
                    }));
                    return {
                        all_groups: all_groups,
                        all_graphs: all_graphs,
                        all_segments: all_segments,
                        all_shards: all_shards
                    };
                })).catch((function(err) {
                    loglevel_default().error(err);
                    message.ZP.error("Please check the console for the error; You need to set up processors_profile_log (ref CK community website) if the error is due to missing table");
                }));
            }
            function getDataFromCache(qid, date) {}
            function ProcessorGraph() {
                var _useParams = (0, react_router_dist.UO)(), qid_date = _useParams.qid_date;
                var _useAuth = useAuth(), auth = _useAuth.auth;
                var _useState = (0, react.useState)(false), _useState2 = (0, slicedToArray.Z)(_useState, 2), loading = _useState2[0], setLoading = _useState2[1];
                var _useState3 = (0, react.useState)(new Map), _useState4 = (0, slicedToArray.Z)(_useState3, 2), graphs = _useState4[0], setGraphs = _useState4[1];
                var _useState5 = (0, react.useState)(new Map), _useState6 = (0, slicedToArray.Z)(_useState5, 2), groups = _useState6[0], setGroups = _useState6[1];
                var _useState7 = (0, react.useState)([]), _useState8 = (0, slicedToArray.Z)(_useState7, 2), segments = _useState8[0], setSegments = _useState8[1];
                var _useState9 = (0, react.useState)([]), _useState10 = (0, slicedToArray.Z)(_useState9, 2), shards = _useState10[0], setShards = _useState10[1];
                var _useState11 = (0, react.useState)("all"), _useState12 = (0, slicedToArray.Z)(_useState11, 2), segment = _useState12[0], setSegment = _useState12[1];
                var _useState13 = (0, react.useState)("all"), _useState14 = (0, slicedToArray.Z)(_useState13, 2), shard = _useState14[0], setShard = _useState14[1];
                var _useState15 = (0, react.useState)(false), _useState16 = (0, slicedToArray.Z)(_useState15, 2), colorExchange = _useState16[0], setColorExchange = _useState16[1];
                var _useState17 = (0, react.useState)(""), _useState18 = (0, slicedToArray.Z)(_useState17, 2), graphDot = _useState18[0], setGraphDot = _useState18[1];
                var qid = qid_date.slice(0, qid_date.length - 11);
                var date = qid_date.slice(-10);
                (0, react.useEffect)((function() {
                    if (auth) {
                        setLoading(true);
                        getDataFromEngine(qid, date, auth.engine).then((function(_ref2) {
                            var all_graphs = _ref2.all_graphs, all_groups = _ref2.all_groups, all_segments = _ref2.all_segments, all_shards = _ref2.all_shards;
                            setGraphs(all_graphs);
                            setGroups(all_groups);
                            setSegments(Array.from(all_segments).sort((function(a, b) {
                                return a - b;
                            })));
                            setShards(Array.from(all_shards).sort((function(a, b) {
                                return a - b;
                            })));
                            loglevel_default().debug("all segments", all_segments);
                            loglevel_default().debug("all shards", all_shards);
                        })).catch((function(err) {
                            console.log(err);
                        }));
                    }
                }), [ auth, qid, date ]);
                (0, react.useEffect)((function() {
                    if (graphs.size && groups.size) {
                        setLoading(true);
                        setTimeout((function() {
                            var dot = processor_toDotString(graphs, groups, segment, shard, colorExchange);
                            setGraphDot(dot);
                            setLoading(false);
                        }), 100);
                    }
                }), [ graphs, groups, segment, shard, colorExchange ]);
                var segment_list = Array.from([ "all" ].concat((0, toConsumableArray.Z)(segments))).map((function(segment) {
                    return {
                        label: "segment ".concat(segment),
                        key: segment,
                        icon: (0, jsx_runtime.jsx)(RightOutlined.Z, {})
                    };
                }));
                var shard_list = Array.from([ "all" ].concat((0, toConsumableArray.Z)(shards))).map((function(shard) {
                    return {
                        label: "shard ".concat(shard),
                        key: shard,
                        icon: (0, jsx_runtime.jsx)(RightOutlined.Z, {})
                    };
                }));
                var chooseSegment = function chooseSegment(e) {
                    resetGraph();
                    setSegment(e.key);
                };
                var chooseShard = function chooseShard(e) {
                    resetGraph();
                    setShard(e.key);
                };
                var node_details_func = function node_details_func(node) {
                    return prepareNodeDetails(node.key, shard, groups);
                };
                return (0, jsx_runtime.jsxs)(spin.Z, {
                    spinning: loading,
                    size: "large",
                    children: [ (0, jsx_runtime.jsxs)("div", {
                        className: "drawer-right",
                        children: [ (0, jsx_runtime.jsx)(dropdown.Z, {
                            menu: {
                                items: segment_list,
                                onClick: chooseSegment
                            },
                            children: (0, jsx_runtime.jsx)(es_button.Z, {
                                children: (0, jsx_runtime.jsxs)(space.Z, {
                                    children: [ segment === "all" ? "Choose a segment" : "Segment ".concat(segment), (0, 
                                    jsx_runtime.jsx)(DownOutlined.Z, {}) ]
                                })
                            })
                        }), (0, jsx_runtime.jsx)(dropdown.Z, {
                            menu: {
                                items: shard_list,
                                onClick: chooseShard
                            },
                            children: (0, jsx_runtime.jsx)(es_button.Z, {
                                children: (0, jsx_runtime.jsxs)(space.Z, {
                                    children: [ shard === "all" ? "Choose a shard" : "Shard ".concat(shard), (0, jsx_runtime.jsx)(DownOutlined.Z, {}) ]
                                })
                            })
                        }), (0, jsx_runtime.jsx)(es_button.Z, {
                            onClick: function onClick() {
                                setColorExchange(!colorExchange);
                            },
                            children: "Toggle Color"
                        }) ]
                    }), (0, jsx_runtime.jsx)(Graph, {
                        graph_dot: graphDot,
                        node_details_func: node_details_func
                    }) ]
                });
            }
            function getStagesFromEngine(qid, date, engine) {
                return engine.execute("Graphviz.Names as stages", "system.query_log", "event_date=toDate('".concat(date, "') and query_id='").concat(qid, "' and is_initial_query=1 and type!= 1")).then((function(rows) {
                    if (rows.length) return Array.from(new Set(rows[0].stages)); else message.ZP.error("There is no query log for qid=".concat(qid));
                })).catch((function(e) {
                    loglevel_default().error(e);
                    message.ZP.error("Check console for the error message; You need to set up the query_log (ref CK community website) in server.xml and set log_queries=1 if the error is due to table missing.");
                }));
            }
            function getGraphvizDotFromEngine(stage, qid, date, engine) {
                return engine.execute("Graphviz['".concat(stage, "'] as dot"), "system.query_log", "event_date=toDate('".concat(date, "') and query_id='").concat(qid, "' and is_initial_query=1 and type!= 1")).then((function(rows) {
                    if (rows.length) {
                        return rows[0].dot;
                    } else {
                        throw Error("No graphviz data found in query log for stage=".concat(stage, " of query qid=").concat(qid, "; please change the settings, e.g., print_graphviz_pipeline, print_graphviz_planner, print_graphviz_ast"));
                    }
                })).catch((function(e) {
                    loglevel_default().error(e);
                    message.ZP.error("Check console for the error message; You need to set up the query_log (ref CK community website) in server.xml and set log_queries=1 if the error is due to table missing.");
                }));
            }
            function OptimizerGraph() {
                var _useParams = (0, react_router_dist.UO)(), qid_date = _useParams.qid_date;
                var _useAuth = useAuth(), auth = _useAuth.auth;
                var _useState = (0, react.useState)(false), _useState2 = (0, slicedToArray.Z)(_useState, 2), loading = _useState2[0], setLoading = _useState2[1];
                var _useState3 = (0, react.useState)([]), _useState4 = (0, slicedToArray.Z)(_useState3, 2), stages = _useState4[0], setStages = _useState4[1];
                var _useState5 = (0, react.useState)("4000-PlanSegment"), _useState6 = (0, slicedToArray.Z)(_useState5, 2), stage = _useState6[0], setStage = _useState6[1];
                var _useState7 = (0, react.useState)(""), _useState8 = (0, slicedToArray.Z)(_useState7, 2), graphDot = _useState8[0], setGraphDot = _useState8[1];
                var qid = qid_date.slice(0, qid_date.length - 11);
                var date = qid_date.slice(-10);
                (0, react.useEffect)((function() {
                    if (auth) {
                        getStagesFromEngine(qid, date, auth.engine).then((function(all_stages) {
                            if (all_stages.length) {
                                setStages(all_stages);
                            } else {
                                message.ZP.error("No graphviz data found in query log for qid=".concat(qid, "; You can try: set print_graphviz=1; set print_graphviz_pipeline=1;"));
                            }
                        }));
                    }
                }), [ auth, qid, date ]);
                (0, react.useEffect)((function() {
                    if (auth && stages.length) {
                        setLoading(true);
                        getGraphvizDotFromEngine(stage, qid, date, auth.engine).then((function(dot) {
                            setGraphDot(dot);
                            setLoading(false);
                        }));
                    }
                }), [ stage, qid, date, stages ]);
                var stage_list = Array.from(stages).map((function(stg) {
                    return {
                        label: stg,
                        key: stg,
                        icon: (0, jsx_runtime.jsx)(RightOutlined.Z, {})
                    };
                }));
                loglevel_default().debug("stage list: ", stage_list);
                var chooseStage = function chooseStage(e) {
                    resetGraph();
                    setStage(e.key);
                    loglevel_default().debug("selected stage: ", e.key);
                };
                return (0, jsx_runtime.jsxs)(spin.Z, {
                    spinning: loading,
                    size: "large",
                    children: [ (0, jsx_runtime.jsx)("div", {
                        className: "drawer-right",
                        children: (0, jsx_runtime.jsx)(dropdown.Z, {
                            menu: {
                                items: stage_list,
                                onClick: chooseStage
                            },
                            children: (0, jsx_runtime.jsx)(es_button.Z, {
                                children: (0, jsx_runtime.jsxs)(space.Z, {
                                    children: [ stage, (0, jsx_runtime.jsx)(DownOutlined.Z, {}) ]
                                })
                            })
                        })
                    }), (0, jsx_runtime.jsx)(Graph, {
                        graph_dot: graphDot
                    }) ]
                });
            }
            var descriptions = __webpack_require__(394);
            var sqlFormatter = __webpack_require__(6607);
            var default_highlight = __webpack_require__(8426);
            var darcula = __webpack_require__(7164);
            var bulma_min = __webpack_require__(6890);
            var overview_Panel = collapse.Z.Panel;
            function formatSQL(sqlStr) {
                loglevel_default().debug("original sql:", sqlStr);
                var formatedSql = "";
                try {
                    formatedSql = (0, sqlFormatter.WU)(sqlStr, {
                        language: "mysql"
                    });
                } catch (error) {
                    loglevel_default().error(error);
                }
                return formatedSql;
            }
            function SQL(_ref) {
                var content = _ref.content, settings = _ref.settings;
                var sqlStr = content.trim();
                if (sqlStr.endsWith(";")) sqlStr = sqlStr.slice(0, sqlStr.length - 1);
                var pos = sqlStr.indexOf("settings");
                if (pos === -1) pos = sqlStr.indexOf("SETTINGS");
                if (pos !== -1) {
                    sqlStr = sqlStr.slice(0, pos);
                }
                var idx = 1;
                var settingStr = "";
                settings.forEach((function(value, name) {
                    settingStr += "".concat(name, "='").concat(value, "', ") + (idx++ % 3 === 0 ? "\n" : "");
                }));
                if (settings.size) {
                    sqlStr += "\nSETTINGS\n".concat(settingStr);
                }
                return (0, jsx_runtime.jsx)(default_highlight.Z, {
                    language: "sql",
                    style: darcula.Z,
                    children: sqlStr
                });
            }
            function overview_getDataFromEngine(qid, date, engine) {
                return engine.execute("query, stack_trace, exception, query_start_time_microseconds as start, \n     event_time_microseconds as end, query_duration_ms, user, type, address, \n     interface, client_name, read_rows, written_rows, read_bytes, written_bytes, \n     result_rows, result_bytes, memory_usage, Settings", "system.query_log", "event_date = toDate('".concat(date, "') and initial_query_id='").concat(qid, "' and type != 1 and is_initial_query = 1")).then((function(rows) {
                    if (rows.length === 0) {
                        throw Error("No record for query_id=".concat(qid, "; Remember to set log_queries=1"));
                    }
                    if (rows.length > 1) {
                        throw Error("There are two records for query_id=".concat(qid));
                    }
                    var details = rows[0];
                    var merged_settings = new Map;
                    for (var _i = 0, _Object$entries = Object.entries(details.Settings); _i < _Object$entries.length; _i++) {
                        var _Object$entries$_i = (0, slicedToArray.Z)(_Object$entries[_i], 2), k = _Object$entries$_i[0], v = _Object$entries$_i[1];
                        if (!engine.settings.has(k) || engine.settings.get(k) !== v) {
                            merged_settings.set(k, v);
                        }
                    }
                    details.settings = merged_settings;
                    return details;
                })).catch((function(e) {
                    loglevel_default().error(e);
                    message.ZP.error("Check console for the error message; You need to set up the query_log (ref CK community website) in server.xml if the error is due to table missing.");
                }));
            }
            function Overview() {
                var _useParams = (0, react_router_dist.UO)(), qid_date = _useParams.qid_date;
                var _useState = (0, react.useState)(), _useState2 = (0, slicedToArray.Z)(_useState, 2), details = _useState2[0], setDetails = _useState2[1];
                var _useAuth = useAuth(), auth = _useAuth.auth;
                var _qid_date$split = qid_date.split("+"), _qid_date$split2 = (0, slicedToArray.Z)(_qid_date$split, 2), qid = _qid_date$split2[0], date = _qid_date$split2[1];
                (0, react.useEffect)((function() {
                    if (auth) {
                        overview_getDataFromEngine(qid, date, auth.engine).then((function(ret) {
                            setDetails(ret);
                        })).catch((function(err) {
                            return message.ZP.error(err.message);
                        }));
                    }
                }), [ qid, date, auth ]);
                var displayNumber = function displayNumber(val) {
                    var suffix = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : "";
                    var isSize = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
                    return "".concat(numberWithComma(val)).concat(suffix, " (").concat(convertUnit(val, isSize)).concat(suffix, ")");
                };
                return (0, jsx_runtime.jsx)("div", {
                    className: "box",
                    children: details && (0, jsx_runtime.jsxs)("div", {
                        children: [ (0, jsx_runtime.jsxs)(descriptions.Z, {
                            title: "Query Information",
                            children: [ (0, jsx_runtime.jsxs)(descriptions.Z.Item, {
                                label: "Latency",
                                children: [ numberWithComma(details.query_duration_ms), "ms" ]
                            }), (0, jsx_runtime.jsx)(descriptions.Z.Item, {
                                label: "Start Time",
                                children: details.start
                            }), (0, jsx_runtime.jsx)(descriptions.Z.Item, {
                                label: "End Time",
                                children: details.end
                            }), (0, jsx_runtime.jsx)(descriptions.Z.Item, {
                                label: "User",
                                children: details.user
                            }), (0, jsx_runtime.jsx)(descriptions.Z.Item, {
                                label: "Read Rows",
                                children: displayNumber(details.read_rows)
                            }), (0, jsx_runtime.jsx)(descriptions.Z.Item, {
                                label: "Read Bytes",
                                children: displayNumber(details.read_bytes, "B", true)
                            }), (0, jsx_runtime.jsx)(descriptions.Z.Item, {
                                label: "Written Rows",
                                children: displayNumber(details.written_rows)
                            }), (0, jsx_runtime.jsx)(descriptions.Z.Item, {
                                label: "Written Bytes",
                                children: displayNumber(details.written_bytes, "B", true)
                            }), (0, jsx_runtime.jsx)(descriptions.Z.Item, {
                                label: "Result Rows",
                                children: displayNumber(details.result_bytes, "B", true)
                            }), (0, jsx_runtime.jsx)(descriptions.Z.Item, {
                                label: "Memory",
                                children: displayNumber(details.memory_usage, "B", true)
                            }) ]
                        }), (0, jsx_runtime.jsxs)(collapse.Z, {
                            defaultActiveKey: [ "1" ],
                            children: [ (0, jsx_runtime.jsx)(overview_Panel, {
                                header: "SQL",
                                children: (0, jsx_runtime.jsx)(SQL, {
                                    title: "Query",
                                    content: formatSQL(details.query || ""),
                                    settings: details.settings
                                })
                            }, "1"), details.trace_stack && (0, jsx_runtime.jsx)(overview_Panel, {
                                header: "Stack Trace",
                                children: (0, jsx_runtime.jsx)(default_highlight.Z, {
                                    language: "bash",
                                    style: darcula.Z,
                                    children: details.stack_trace
                                })
                            }, "2"), details.exception && (0, jsx_runtime.jsx)(overview_Panel, {
                                header: "Exception",
                                children: (0, jsx_runtime.jsx)(default_highlight.Z, {
                                    language: "bash",
                                    style: darcula.Z,
                                    children: details.exception
                                })
                            }, "3") ]
                        }) ]
                    })
                });
            }
            function events_getDataFromEngine(qid, date, engine) {
                return engine.execute("query_id, ProfileEvents.Names as names, ProfileEvents.Values as values", "system.query_log", "event_date = toDate('".concat(date, "') and initial_query_id='").concat(qid, "' and type != 1")).then((function(rows) {
                    if (rows.length === 0) {
                        throw Error("No record for query_id=".concat(qid));
                    }
                    var events = new Map;
                    rows.forEach((function(row) {
                        var pos = row.query_id.lastIndexOf("_");
                        var segment = "all";
                        if (pos > 0) {
                            segment = row.query_id.slice(pos + 1);
                        } else {
                            segment = "server";
                        }
                        var shard = row.shard;
                        row.names.forEach((function(name, idx) {
                            var value = +row.values[idx];
                            if (!events.has(name)) {
                                events.set(name, {
                                    event: name,
                                    segment: "all",
                                    shard: "all",
                                    value: 0,
                                    key: name,
                                    children: new Map
                                });
                            }
                            var event = events.get(name);
                            event.value += value;
                            if (!event.children.has(segment)) {
                                event.children.set(segment, {
                                    event: name,
                                    segment: segment,
                                    shard: "all",
                                    value: 0,
                                    key: "".concat(name, "-").concat(event.children.size),
                                    children: new Map
                                });
                            }
                            var segment_event = event.children.get(segment);
                            segment_event.value += value;
                            if (!segment_event.children.has(shard)) {
                                segment_event.children.set(row.shard, {
                                    event: name,
                                    segment: segment,
                                    shard: shard,
                                    key: "".concat(name, "-").concat(event.children.size, "-").concat(segment_event.children.size),
                                    value: 0
                                });
                            }
                            var shard_event = segment_event.children.get(shard);
                            shard_event.value += value;
                        }));
                    }));
                    events = Array.from(events.values());
                    events.forEach((function(event) {
                        event.children = Array.from(event.children.values());
                        event.children.forEach((function(segment_event) {
                            segment_event.children = Array.from(segment_event.children.values());
                        }));
                    }));
                    loglevel_default().debug(events);
                    var EventMetrics = [ [ "event", "Event", "", "String" ], [ "segment", "Segment", "Segment=server indicates the events are recorded at the server", "String" ], [ "shard", "Worker", "", "String" ], [ "value", "Value", "", "Int" ] ];
                    return prepareTable(EventMetrics, events);
                })).catch((function(err) {
                    loglevel_default().error(err);
                    message.ZP.error("Check console for the error message; You need to set up the query_log (ref CK community website) in server.xml if the error is due to table missing.");
                }));
            }
            function ProfileEvents() {
                var _useParams = (0, react_router_dist.UO)(), qid_date = _useParams.qid_date;
                var _useState = (0, react.useState)(), _useState2 = (0, slicedToArray.Z)(_useState, 2), events = _useState2[0], setEvents = _useState2[1];
                var _useState3 = (0, react.useState)(true), _useState4 = (0, slicedToArray.Z)(_useState3, 2), loading = _useState4[0], setLoading = _useState4[1];
                var _useAuth = useAuth(), auth = _useAuth.auth;
                var _qid_date$split = qid_date.split("+"), _qid_date$split2 = (0, slicedToArray.Z)(_qid_date$split, 2), qid = _qid_date$split2[0], date = _qid_date$split2[1];
                (0, react.useEffect)((function() {
                    if (auth) {
                        setLoading(true);
                        events_getDataFromEngine(qid, date, auth.engine).then((function(ret) {
                            setEvents(ret);
                            setLoading(false);
                        })).catch((function(err) {
                            message.ZP.error(err.message);
                            setLoading(false);
                        }));
                    }
                }), [ qid, date, auth ]);
                return (0, jsx_runtime.jsx)("div", {
                    style: {
                        margin: 10
                    },
                    children: (0, jsx_runtime.jsx)(spin.Z, {
                        spinning: loading,
                        size: "large",
                        children: events && (0, jsx_runtime.jsx)(es_table.Z, {
                            className: "table is-fullwidth",
                            pagination: false,
                            columns: events.columns,
                            dataSource: events.dataSource
                        })
                    })
                });
            }
            function System() {
                var _useState = (0, react.useState)(null), _useState2 = (0, slicedToArray.Z)(_useState, 2), options = _useState2[0], setOptions = _useState2[1];
                var _useParams = (0, react_router_dist.UO)(), target = _useParams.target;
                var _useAuth = useAuth(), auth = _useAuth.auth;
                (0, react.useEffect)((function() {
                    if (auth) {
                        var table = null;
                        if (target === "version") table = "system.build_options"; else if (target === "setting") table = "system.settings";
                        executeQuery("select * from ".concat(table), auth, auth.engine.dialect_type).then((function(json) {
                            setOptions(json);
                        })).catch((function(err) {
                            loglevel_default().error(err);
                        }));
                    }
                }), [ auth, target ]);
                if (options) {
                    return (0, jsx_runtime.jsxs)("table", {
                        style: {
                            margin: 20
                        },
                        children: [ (0, jsx_runtime.jsx)("thead", {
                            children: (0, jsx_runtime.jsx)("tr", {
                                children: options.meta.map((function(col) {
                                    return (0, jsx_runtime.jsx)("th", {
                                        children: col.name
                                    }, col.name);
                                }))
                            })
                        }), (0, jsx_runtime.jsx)("tbody", {
                            children: options.data.map((function(row, idx) {
                                return (0, jsx_runtime.jsx)("tr", {
                                    children: options.meta.map((function(col) {
                                        return (0, jsx_runtime.jsx)("td", {
                                            children: row[col.name]
                                        }, col.name);
                                    }))
                                }, idx);
                            }))
                        }) ]
                    });
                } else {
                    return null;
                }
            }
            var result = __webpack_require__(7063);
            function RootError() {
                return (0, jsx_runtime.jsx)(result.ZP, {
                    status: "warning",
                    title: "There are some problems with your operation. Please check console logs"
                });
            }
            function QueryError() {
                return (0, jsx_runtime.jsx)(result.ZP, {
                    status: "warning",
                    title: "There are some problems with your operation. Please check console logs"
                });
            }
            var App = {};
            var router = (0, dist.aj)((0, react_router_dist.i7)((0, jsx_runtime.jsxs)(react_router_dist.AW, {
                path: "/",
                element: (0, jsx_runtime.jsx)(Layout, {}),
                errorElement: (0, jsx_runtime.jsx)(RootError, {}),
                children: [ (0, jsx_runtime.jsx)(react_router_dist.AW, {
                    index: true,
                    path: "history",
                    element: (0, jsx_runtime.jsx)(RequireAuth, {
                        children: (0, jsx_runtime.jsx)(History, {})
                    })
                }), (0, jsx_runtime.jsx)(react_router_dist.AW, {
                    path: "starrocks",
                    element: (0, jsx_runtime.jsx)(StarRocks, {})
                }), (0, jsx_runtime.jsx)(react_router_dist.AW, {
                    path: "system/:target",
                    element: (0, jsx_runtime.jsx)(RequireAuth, {
                        children: (0, jsx_runtime.jsx)(System, {})
                    })
                }), (0, jsx_runtime.jsxs)(react_router_dist.AW, {
                    path: "login",
                    children: [ (0, jsx_runtime.jsx)(react_router_dist.AW, {
                        index: true,
                        element: (0, jsx_runtime.jsx)(Login, {})
                    }), (0, jsx_runtime.jsx)(react_router_dist.AW, {
                        path: ":instruction",
                        element: (0, jsx_runtime.jsx)(Login, {})
                    }) ]
                }), (0, jsx_runtime.jsxs)(react_router_dist.AW, {
                    path: "query/:qid_date",
                    element: (0, jsx_runtime.jsx)(RequireAuth, {
                        children: (0, jsx_runtime.jsx)(Query, {})
                    }),
                    errorElement: (0, jsx_runtime.jsx)(QueryError, {}),
                    children: [ (0, jsx_runtime.jsx)(react_router_dist.AW, {
                        index: true,
                        element: (0, jsx_runtime.jsx)(Overview, {})
                    }), (0, jsx_runtime.jsx)(react_router_dist.AW, {
                        path: "optimizer",
                        element: (0, jsx_runtime.jsx)(OptimizerGraph, {})
                    }), (0, jsx_runtime.jsx)(react_router_dist.AW, {
                        path: "processor",
                        element: (0, jsx_runtime.jsx)(ProcessorGraph, {})
                    }), (0, jsx_runtime.jsx)(react_router_dist.AW, {
                        path: "profile_events",
                        element: (0, jsx_runtime.jsx)(ProfileEvents, {})
                    }) ]
                }) ]
            })), {
                basename: "/profiler"
            });
            client.createRoot(document.getElementById("root")).render((0, jsx_runtime.jsx)(react.StrictMode, {
                children: (0, jsx_runtime.jsx)(AuthProvider, {
                    children: (0, jsx_runtime.jsx)(react_router_dist.pG, {
                        router: router
                    })
                })
            }));
        }
    };
    var __webpack_module_cache__ = {};
    function __webpack_require__(moduleId) {
        var cachedModule = __webpack_module_cache__[moduleId];
        if (cachedModule !== undefined) {
            return cachedModule.exports;
        }
        var module = __webpack_module_cache__[moduleId] = {
            id: moduleId,
            loaded: false,
            exports: {}
        };
        __webpack_modules__[moduleId].call(module.exports, module, module.exports, __webpack_require__);
        module.loaded = true;
        return module.exports;
    }
    __webpack_require__.m = __webpack_modules__;
    !function() {
        var deferred = [];
        __webpack_require__.O = function(result, chunkIds, fn, priority) {
            if (chunkIds) {
                priority = priority || 0;
                for (var i = deferred.length; i > 0 && deferred[i - 1][2] > priority; i--) deferred[i] = deferred[i - 1];
                deferred[i] = [ chunkIds, fn, priority ];
                return;
            }
            var notFulfilled = Infinity;
            for (var i = 0; i < deferred.length; i++) {
                var chunkIds = deferred[i][0];
                var fn = deferred[i][1];
                var priority = deferred[i][2];
                var fulfilled = true;
                for (var j = 0; j < chunkIds.length; j++) {
                    if ((priority & 1 === 0 || notFulfilled >= priority) && Object.keys(__webpack_require__.O).every((function(key) {
                        return __webpack_require__.O[key](chunkIds[j]);
                    }))) {
                        chunkIds.splice(j--, 1);
                    } else {
                        fulfilled = false;
                        if (priority < notFulfilled) notFulfilled = priority;
                    }
                }
                if (fulfilled) {
                    deferred.splice(i--, 1);
                    var r = fn();
                    if (r !== undefined) result = r;
                }
            }
            return result;
        };
    }();
    !function() {
        __webpack_require__.n = function(module) {
            var getter = module && module.__esModule ? function() {
                return module["default"];
            } : function() {
                return module;
            };
            __webpack_require__.d(getter, {
                a: getter
            });
            return getter;
        };
    }();
    !function() {
        var getProto = Object.getPrototypeOf ? function(obj) {
            return Object.getPrototypeOf(obj);
        } : function(obj) {
            return obj.__proto__;
        };
        var leafPrototypes;
        __webpack_require__.t = function(value, mode) {
            if (mode & 1) value = this(value);
            if (mode & 8) return value;
            if (typeof value === "object" && value) {
                if (mode & 4 && value.__esModule) return value;
                if (mode & 16 && typeof value.then === "function") return value;
            }
            var ns = Object.create(null);
            __webpack_require__.r(ns);
            var def = {};
            leafPrototypes = leafPrototypes || [ null, getProto({}), getProto([]), getProto(getProto) ];
            for (var current = mode & 2 && value; typeof current == "object" && !~leafPrototypes.indexOf(current); current = getProto(current)) {
                Object.getOwnPropertyNames(current).forEach((function(key) {
                    def[key] = function() {
                        return value[key];
                    };
                }));
            }
            def["default"] = function() {
                return value;
            };
            __webpack_require__.d(ns, def);
            return ns;
        };
    }();
    !function() {
        __webpack_require__.d = function(exports, definition) {
            for (var key in definition) {
                if (__webpack_require__.o(definition, key) && !__webpack_require__.o(exports, key)) {
                    Object.defineProperty(exports, key, {
                        enumerable: true,
                        get: definition[key]
                    });
                }
            }
        };
    }();
    !function() {
        __webpack_require__.g = function() {
            if (typeof globalThis === "object") return globalThis;
            try {
                return this || new Function("return this")();
            } catch (e) {
                if (typeof window === "object") return window;
            }
        }();
    }();
    !function() {
        __webpack_require__.o = function(obj, prop) {
            return Object.prototype.hasOwnProperty.call(obj, prop);
        };
    }();
    !function() {
        __webpack_require__.r = function(exports) {
            if (typeof Symbol !== "undefined" && Symbol.toStringTag) {
                Object.defineProperty(exports, Symbol.toStringTag, {
                    value: "Module"
                });
            }
            Object.defineProperty(exports, "__esModule", {
                value: true
            });
        };
    }();
    !function() {
        __webpack_require__.nmd = function(module) {
            module.paths = [];
            if (!module.children) module.children = [];
            return module;
        };
    }();
    !function() {
        var installedChunks = {
            179: 0
        };
        __webpack_require__.O.j = function(chunkId) {
            return installedChunks[chunkId] === 0;
        };
        var webpackJsonpCallback = function(parentChunkLoadingFunction, data) {
            var chunkIds = data[0];
            var moreModules = data[1];
            var runtime = data[2];
            var moduleId, chunkId, i = 0;
            if (chunkIds.some((function(id) {
                return installedChunks[id] !== 0;
            }))) {
                for (moduleId in moreModules) {
                    if (__webpack_require__.o(moreModules, moduleId)) {
                        __webpack_require__.m[moduleId] = moreModules[moduleId];
                    }
                }
                if (runtime) var result = runtime(__webpack_require__);
            }
            if (parentChunkLoadingFunction) parentChunkLoadingFunction(data);
            for (;i < chunkIds.length; i++) {
                chunkId = chunkIds[i];
                if (__webpack_require__.o(installedChunks, chunkId) && installedChunks[chunkId]) {
                    installedChunks[chunkId][0]();
                }
                installedChunks[chunkId] = 0;
            }
            return __webpack_require__.O(result);
        };
        var chunkLoadingGlobal = self["webpackChunk_byted_clickhouse_profiler"] = self["webpackChunk_byted_clickhouse_profiler"] || [];
        chunkLoadingGlobal.forEach(webpackJsonpCallback.bind(null, 0));
        chunkLoadingGlobal.push = webpackJsonpCallback.bind(null, chunkLoadingGlobal.push.bind(chunkLoadingGlobal));
    }();
    var __webpack_exports__ = __webpack_require__.O(undefined, [ 216 ], (function() {
        return __webpack_require__(5378);
    }));
    __webpack_exports__ = __webpack_require__.O(__webpack_exports__);
})();
//# sourceMappingURL=main.js.map