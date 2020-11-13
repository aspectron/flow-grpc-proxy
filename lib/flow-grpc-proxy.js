const path 	= require("path");
const fs 	= require("fs");
const http = require('http');
const https = require('https');
const EventEmitter = require("events");
const crypto = require("crypto");
const FlowUid = require("@aspectron/flow-uid");
const utils = require("./utils");

class FlowGRPCProxy extends EventEmitter{

	static METHODS = Object.freeze({
		PUBLISH : 1,
		REQUEST : 2,
		SUBSCRIBE : 3
	})
	
	constructor(appFolder, options={}){
		super();
		this.appFolder_ = appFolder;
		this.initLog();
		this.pkg = require(path.join(this.appFolder, "package.json"));
		this.options = Object.assign({}, this.defaultOptions, options);
		this._log = {
	        'INFO' : 1,
	        'WARN' : 2,
	        'DEBUG' : 3
	    }
		this.rtUID = FlowUid({ length : 16 });
		this.debug = true;
	}

	async init(){
		await this._init();
	}
	async _init(){
		this.initConfig();

		if(this.config.certificates)
			await this.initCertificates();
		await this.initHttp(this);
		await this.initGRPC();
	}

	initConfig(){
		this.config = {}
		let {configFile, config} = this.options;
		if(config){
			this.config = config;
			return;
		}
		if(!configFile || !fs.existsSync(configFile))
			return
		this.config = utils.getConfig(configFile);
	}

	async initGRPC() {
		const {json, proto, grpc, server} = this.parseProto(this.config.grpc||{});

		this.grpcClients = {};
		utils.each(json.services, (value, name)=>{
			this.grpcClients[name] = new proto[name](server,
						grpc.credentials.createInsecure());
		});
		this.grpcProto = json;

		this.testRPC(this.grpcClients.RPC);
	}
	testRPC(client){
		//var client = new RPC('localhost:16210', gRPC.credentials.createInsecure());
		//console.log("client", client)
		//let reqStream = {getBlockDagInfoRequest:{}};
		let stream = client.MessageStream((...args)=>{
			console.log("MessageStream fn", args)
		});

		//console.log("stream", stream);

		stream.on('metadata', function(...args) {
			console.log('stream metadata', args);
		});
		stream.on('status', function(...args) {
			console.log('stream status', args);
		});
		stream.on('data', function(...args) {
			console.log('stream data', args);
			stream.end();
		});
		stream.on('end', (a, b)=>{
			console.log('stream end', a, b);
		});
		stream.on('finish', (a, b)=>{
			console.log('stream finish', a, b);
		});
		let req = {
			getUTXOsByAddressRequest:{}
			//getBlockDagInfoRequest:{}
		}
		stream.write(req);
	}

	createStreamSubscription({client, method, sId, socketId, data}, callback){
		if(!client._streams)
			client._streams = {};
		if(!client._streams[method]){
			const stream = client[method]();
			client._streams[method] = stream
			stream._callbacks = [];
			stream._socketIds = new Set();
			stream.on('data', (data)=>{
				//stream.end();
				let cb = stream._callbacks.shift();
				console.log('stream data', data, cb);
				if(cb){
					cb(null, data);
				}
			});
			stream.on('end', ()=>{
				console.log('stream end');
				stream._socketIds.forEach(socketId=>{
					let socket = this.websocketMap.get(socketId)
					if(socket){
						let sIds = [...socket._sIds.values()];
						socket.emit("message", {subject:"grpc.stream.end", data:{sIds}})
					}
				})
			});

			/*
			stream.on('error', (error)=>{
				console.log('stream error', error);
				stream._socketIds.forEach(socketId=>{
					let socket = this.websocketMap.get(socketId)
					if(socket){
						let sIds = [...socket._sIds.values()];
						socket.emit("message", {subject:"grpc.stream.error", data:{sIds, error}})
					}
				})
			});
			*/
		}

		let socket = this.websocketMap.get(socketId);
		if(!socket._sIds)
			socket._sIds = new Set();
		socket._sIds.add(sId);

		const stream = client._streams[method];
		stream._socketIds.add(socketId);
		stream._callbacks.push(callback);
		stream.write(data);

	}

	parseProto(config){
		const { grpc, protoLoader} = FlowGRPCProxy.modules;
		if(!grpc || !protoLoader)
			throw new Error("FlowGRPCProxy requires grpc and protoLoader modules.");

		const {protoPath, server, packageKey, loaderConfig={}} = config;
		const params = Object.assign({
			keepCase: true,
			longs: String,
			enums: String,
			defaults: true,
			oneofs: true
		}, loaderConfig);

		const packageDefinition = protoLoader.loadSync(protoPath, params);
		const proto = grpc.loadPackageDefinition(packageDefinition)[packageKey];
		//const client = new proto.RPC(server,
						//grpc.credentials.createInsecure());
		const json = {services:{}, msg:{}};
		utils.each(proto, (value, key)=>{
			if(value.service){
				json.services[key] = JSON.parse(JSON.stringify(value.service));
				utils.each(json.services[key], (value, method)=>{
					delete value.requestType.fileDescriptorProtos;
					delete value.responseType.fileDescriptorProtos;
					//console.log("key:method:value", method, value)
				})
			}else{
				value = JSON.parse(JSON.stringify(value));
				json.msg[key] = value;
				delete value.fileDescriptorProtos;
			}
			//value = JSON.parse(JSON.stringify(value))
			//console.log("proto, key:value", key, JSON.stringify(value.service, null, "\t"))
		})
		//console.log("json", JSON.stringify(json, null, "  "))
		return {json, proto, grpc, server}
	}

	initHttp(){
		return new Promise((resolve, reject)=>{
			this.initExpressApp();
			this.emit("init::app", {app:this.app});
			this.initStaticFiles();

			this.initHttpServer(resolve);
			
		}).then(()=>{

		}, (err)=>{
			this.log("initHttp:err", err)
		})
	}

	initHttpServer(resolve){
		let {config, options} = this;
		let {http:httpConfig} = config;
		if(!httpConfig){
			this.initIO(options.io);
			resolve();
			return
		}

		let {port, host, ssl} = httpConfig;

		http.globalAgent.maxSockets = config.maxHttpSockets || 1024;
		https.globalAgent.maxSockets = config.maxHttpSockets || 1024;

		let CERTIFICATES = ssl && this.certificates;

		let args = [ ]
		args.push(port);
		host && args.push(host);

		args.push(err=>{
			if(err){
				console.error(`Unable to start HTTP(S) server on port ${port}  ${host?" host '"+host+"'":''}`);
				return reject(err);
			}

			this.log(`HTTP server listening on port ${(port+'')}  ${host?" host '"+host+"'":''}`);

			if (!CERTIFICATES)
				this.log(("WARNING - SSL is currently disabled"));

			this.emit('init::http-server', {server:this.server})
			resolve();
		})

		let server;
		if(CERTIFICATES){
			server = https.createServer(CERTIFICATES, this.app)
			this._isSecureServer = true;
		}else{
			server = http.createServer(this.app)
		}
		this.server = server;

		// ---
		const { socketio } = FlowGRPCProxy.modules;
		if(socketio) {

			server.on("upgrade", (req, socket, head) => {
				if(!this.io)
					return
				this.io.engine.ws.once("headers", headers=>{
					let sessionCookie = this.buildSesssionCookie(req, headers);
					if(sessionCookie)
						headers[headers.length] = "Set-Cookie: "+sessionCookie;
				})
			})
			const io = socketio.listen(server, {
				'log level': 0,
				'secure': CERTIFICATES ? true : false,
				allowRequest:(req, fn)=>{
					if(this.config.handleWSSession) {
						this.allowWSRequest(req, fn);
					} else {
						fn(null, true);
					}
				}
			});
			this.initIO(io);
		}

		server.listen.apply(server, args);
	}

	initIO(io){
		if(!io)
			return;
		this.io = io;
		//if(this.config.websocketPath)
			this.init_socketio_handler(this.config.websocketPath||"grpc", this.config.websocketMode || 'GRPC');
	}

    get defaultOptions(){
		let pkg = this.pkg;
		let ident = pkg.appIdent || pkg.name;
		return {
			ident,
			configFile:path.join(this.appFolder, `config/${ident}.conf`)
		}
	}

	get appFolder(){
		return this.appFolder_;//process.cwd();
	}

	initLog(){
		let name = this.constructor.name;
		let logPrefix = this.logPrefix || `[${name}]:`;
		this.log = console.log.bind(console, logPrefix)
	}

	init_socketio_handler(websocketPath, websocketMode) {
		const NAX_SUBSCRIPTIONS = 64;
		let socketsOpen = 0;
		this.websocketMap = new Map();
		this.tokenToSocketMap = new Map();
		this.subscriptionMap = new Map();
		this.pendingMap = new Map();
		const subscriptionTokenMap = new Map();

		this.websockets = this.io.of(websocketPath).on('connection', async (socket)=>{
			let session = {};
			/*
			let session = await this.getSocketSession(socket)
			.catch(err=>{
				this.log("getSocketSession:error", err)
			});
			
			session = session || {};
			socket.session = session;
			if(!session.user)
				session.user = { token : null }

			console.log("#### socket:init", socket.id, session)

			if(session.user.token)
				this.addSocketIdToTokenSocketMap(session.user.token, socket.id);
			*/


			socketsOpen++;
			let rids = 0;
			this.websocketMap.set(socket.id, socket);
			if(!this.subscriptionMap.has(socket.id))
				this.subscriptionMap.set(socket.id, []); 
			const subscriptions = this.subscriptionMap.get(socket.id);

			if(!this.pendingMap.has(socket.id))
				this.pendingMap.set(socket.id, new Map())
			const pending = this.pendingMap.get(socket.id);
			
			socket.emit('message', {subject:'init'});
			//socket.emit('message', {subject:'grpc.proto', data:{proto:this.grpcProto}});
			socket.on('grpc.proto.get', ()=>{
				//console.log("grpc.proto.get", this.grpcProto)
				socket.emit('message', {subject:'grpc.proto', data:{proto:this.grpcProto}});
			})
			
			this.emit("websocket.connect", {socket});

			socket.on('disconnect', ()=>{
				if(session.user?.token) {
					let socket_id_set = this.tokenToSocketMap.get(session.user.token);
					if(socket_id_set) {
						socket_id_set.delete(socket.id);
						if(!socket_id_set.size)
							this.tokenToSocketMap.delete(session.user.token);
					}
				}
				this.websocketMap.delete(socket.id);
				while(subscriptions.length) {
					const { token, subscription } = subscriptions.shift();
					subscriptionTokenMap.delete(token);
					subscription.unsubscribe();
					//this.nats.unsubscribe();
				}
				this.subscriptionMap.delete(socket.id);
				
				pending.forEach(cb=>{
					cb('disconnect');
				});

				pending.clear();

				socketsOpen--;
			});


			socket.on('message', (msg, callback)=>{
				try {
					let { subject, data } = msg;
					this.emit(subject, data, { subject, socket, rpc : this });
				}
				catch(ex) {
					console.error(ex.stack);
				}
			});

			socket.on('response', (msg) => {
				let { resp, rid } = msg;
				if(pending.has(rid)) {
					let cb = pending.get(rid);
					pending.delete(rid);
					cb(null, resp);
				}					
			});

			socket.on('publish', (msg) => {
				// TODO - check token, reject or publish to NATS
				let { req : { subject, data }, ack, rid } = msg;

				if(!data)
					data = { };
				if(!this.checkAuth(session.user, subject, data, FlowGRPCProxy.METHODS.PUBLISH)) {
					socket.emit('publish::response', { rid, error: "Access Denied" });
					return;
				}

				if(!ack)
					return this.nats.publish(subject, jc.encode(data));

				this.nats.publish(subject, jc.encode(data));
				socket.emit('publish::response', { rid, ack : true });
			});

			socket.on('unsubscribe', (msg) => {
				// TODO - sanity checks
				if(!msg || !msg.req) {
					socket.emit('unsubscribe::response', { rid, error : 'malformed request' });
					return;
				}


				let { req : { token }, rid } = msg;

				let sub = subscriptionTokenMap.get(token);
				if(!sub) {
					socket.emit('unsubscribe::response', { rid, error : 'no such token' });
					return;
				}

				const { subscription } = sub;
				subscriptionTokenMap.delete(token);
				subscription.unsubscribe();
				socket.emit('unsubscribe::response', { rid, ok : true });
			});

			// NATS subscribe
			socket.on('subscribe', (msg) => {
				// TODO - sanity checks
				// console.log('subscribe msg',msg);
				if(!msg || !msg.req || !msg.req.subject) {
					socket.emit('subscribe::response', { rid, error : 'malformed request' });
					return;
				}

				let { req : { subject, opt }, rid } = msg;
				const d_ = { };
				if(!this.checkAuth(session.user, subject, d_, FlowGRPCProxy.METHODS.SUBSCRIBE)) {
					socket.emit('subscribe::response', { rid, error: "Access Denied" });
					return;
				}

				if(subscriptions.length > NAX_SUBSCRIPTIONS) {
					socket.emit('subscribe::response', { rid, error: "Maximum Number of Subscriptions Reached" });
					return;
				}

				//this.nats.publish(subject,message);

				console.log('subscribing subject:',subject);
				const subscription = this.nats.subscribe(subject);
				(async () => {
					for await(const msg of subscription) {
						const data = jc.decode(msg.data);
						if(msg.reply) {
							const subject = msg.subject;
							
							const rid = rids++;
							socket.emit('request', { rid, req : { subject, data }});
							pending.set(rid, (error, data) => {
								if(error)
									msg.respond(jc.encode({ error }));
								else
									msg.respond(jc.encode(data));
							})
						}

						socket.emit('publish', { subject, data });							
					}
				})().then();

				// subscriptions
				let token = FlowUid({ length : 24 });
				subscriptions.push({ token, subscription });
				subscriptionTokenMap.set(token, subscription);

				socket.emit('subscribe::response', { rid, token, subject });
			});

			socket.on('grpc.stream.write', (msg)=>{
				let { req : { client:clientName, method, data }, sId, rid } = msg;
				this.debug && console.log('got grpc.stream.write:', method, '->', msg);
				const client = this.grpcClients[clientName];
				if(!client)
					return socket.emit('grpc.stream.response', { rid, sId, error: `No such service client "${clientName}".` });

				this.debug && console.log('allowing request:', method, '->', data);
				console.log(`typeof this.grpc.${clientName}.${method}`, typeof client[method])

				if(typeof client[method] != "function"){
					socket.emit('grpc.stream.response', { rid, sId, error: `${method} function not found` });
					return
				}


				this.createStreamSubscription({
					client, method,
					sId, data, socketId:socket.id
				}, (error, response)=>{
					if(error){
						socket.emit('grpc.stream.response', {rid, sId, error});
						return
					}

					socket.emit('grpc.stream.response', {rid, sId, response});
				});

			})

			socket.on('grpc.request', (msg) => {

				let { req : { client:clientName, method, data }, rid } = msg;
				this.debug && console.log('got grpc.request:', method, '->', msg);

				if(this.messageFilter_ && !this.messageFilter_(method)) {
					socket.emit('grpc.response', { rid, error: "Unknown Message" });
					return;
				}
				if(!data)
					data = { };
				if(!this.checkAuth(session.user, method, data, FlowGRPCProxy.METHODS.REQUEST)) {
					socket.emit('grpc.response', { rid, error: "Access Denied" });
					return;
				}
				const client = this.grpcClients[clientName];
				if(!client)
					return socket.emit('grpc.response', { rid, error: `No such service client "${clientName}".` });

				this.debug && console.log('allowing request:', method, '->', data);
				console.log(`typeof this.grpc.${clientName}.${method}`, typeof client[method])

				if(typeof client[method] != "function"){
					socket.emit('grpc.response', { rid, error: `${method} function not found` });
					return
				}
				client[method](data, (error, response) => {

					this.debug && console.log("+++++++++++++++++++++++++++++++++++++++++");
					this.debug && console.log('got response:', method, '->', error, response);
					if(!error)
						this.handleResponse(socket.id, session.user, method, response, session);

					socket.emit('grpc.response', {rid, error, response});
				})
			});

		});
	}

	messageFilter(filter) {
		this.messageFilter_ = filter;
	}
	preflight(preflight) {
		this.preflight_ = preflight;
	}

	checkAuth(user, subject, data, method) {
		return true;		
	}

	handleResponse(socket_id, user, subject, response, session) {
		
	}

	addSocketIdToTokenSocketMap(token, socketId){
		let socket_id_set = this.tokenToSocketMap.get(token);
		if(!socket_id_set) {
			socket_id_set = new Set();
			this.tokenToSocketMap.set(token, socket_id_set);
		}
		socket_id_set.add(socketId);
	}


	initExpressApp(){
		let {config, options} = this;
		if(options.app || options.app === false){
			this.app = options.app;
			return;
		}
		let {express} = FlowGRPCProxy.modules;
		if(typeof express!= 'function')
			throw new Error("FlowGRPCProxy requires express module.");
		let app = express();
		this.app = app;
		let {xFrameOptions="SAMEORIGIN"} = config.http||{};
		if(xFrameOptions){
			app.use((req, res, next)=>{
				if(req.url == "/" || req.url == "/index.html")
					res.setHeader("X-Frame-Options", xFrameOptions);
				next();
			})
		}
		//this.initSession(app);
	}

	initStaticFiles(){
		let {express} = FlowGRPCProxy.modules;
		if(!this.config.staticFiles || !express || !this.app)
			return
		
		let ServeStatic = express.static;
		utils.each(this.config.staticFiles, (dst, src)=>{
			console.log('HTTP serving '+src+' -> '+dst);
			this.app.use(src, ServeStatic(path.join(this.appFolder, dst)));
		})
	}

	initCertificates(){
		if(this.verbose)
			console.log('FlowGRPCProxy: loading certificates from ', this.appFolder+'/'+this.config.certificates);
		if(this.certificates) {
			console.error("Warning! initCertificates() is called twice!");
			return;
		}

		let {config} = this;
		let ca_chain;
		if(typeof(config.certificates) == 'string') {
			this.certificates = {
				key: fs.readFileSync(this.locateCertificateFile(config.certificates+'.key')).toString(),
				cert: fs.readFileSync(this.locateCertificateFile(config.certificates+'.crt')).toString(),
				ca: [ ]
			}
			ca_chain = config.certificates+'.ca';
		}else{
			this.certificates = {
				key: fs.readFileSync(this.locateCertificateFile(config.certificates.key)).toString(),
				cert: fs.readFileSync(this.locateCertificateFile(config.certificates.crt)).toString(),
				ca: [ ]
			}
			ca_chain = config.certificates.ca;
		}

		if(ca_chain) {
			let ca_chain_file = this.locateCertificateFile(ca_chain, true);

			if(ca_chain_file) {
				let cert = [ ]
				let chain = fs.readFileSync(ca_chain_file).toString().split('\n');
				chain.forEach(line=>{
					cert.push(line);
					if(line.match('/-END CERTIFICATE-/')) {
						this.certificates.ca.push(cert.join('\n'));
						cert = [ ]
					}
				})
			}
		}
	}

	locateCertificateFile(filename, ignore) {
		let file = path.join(appFolder, filename);
		let parts = file.split('.');
		parts.splice(parts.length-1, 0, '.local');
		let local = parts.join();
		if(fs.existsSync(local))
			return local;
		if(!ignore && !fs.existsSync(file)) {
			this.log("Unable to locate certificate file:", file);
			throw new Error("Unable to locate certificate file");
		}
		else if(ignore && !fs.existsSync(file))
			return null;

		return file;
	}

	/*
	initAccess(prefix, publicFilter, privateFilter) {
		this.MSG = Object.freeze({
			auth : `${prefix}.auth`,
			signup: `${prefix}.signup`,
			authClose : `${prefix}.auth.close`,
			bind : `${prefix}.bind`,
			unbind : `${prefix}.unbind`,
			cast : `${prefix}.cast.${this.rtUID}`,
		});
		this.publicFilter_ = publicFilter;
		this.privateFilter_ = privateFilter;
	}
	unsignCookies(obj, secret){
		let {CookieSignature} = FlowGRPCProxy.modules;
		if(!CookieSignature)
			throw new Error("CookieSignature module is required.");

        let ret = {};
        Object.keys(obj).forEach(key=>{
            let val = obj[key];
            if (0 == val.indexOf('s:')) {
                val = CookieSignature.unsign(val.slice(2), secret);
                if (val) {
                    ret[key] = val;
                    delete obj[key];
                }
            }
        });
        return ret;
    }

	buildSesssionCookie(req){
		let {Cookie, CookieSignature} = FlowGRPCProxy.modules;
        if(!Cookie || !CookieSignature || !req.session || !req.sessionID)
            return false;

        let cookieName          = this.getHttpSessionCookieName();
        let signed              = 's:'+CookieSignature.sign(req.sessionID, this.getHttpSessionSecret());

        return Cookie.serialize(cookieName, signed, req.session.cookie);
    }

	allowWSRequest(req, fn){
        let res = req.res;

        if(res){
            let _writeHead = res.writeHead;
            res.writeHead = (statusCode, statusMessage, headers)=>{
                if(!headers){
                    headers = statusMessage;
                    statusMessage = null;
                }

                headers = headers || {};

                let cookies = headers["Set-Cookie"] || [];
                if(!utils.isArray(cookies))
                    cookies = [cookies];

                let sessionCookie = this.buildSesssionCookie(req, headers);
                if(sessionCookie)
                    cookies.push(sessionCookie);
                //console.log("cookies", cookies)

                headers["Set-Cookie"] = cookies;

                if(statusMessage)
                    _writeHead.call(res, statusCode, statusMessage, headers);
                else
                    _writeHead.call(res, statusCode, headers);
            }
        }else{
            res = {
                end(){

                }
            }
        }
        this.expressSession(req, res, ()=>{
            fn(null, true);
        })   
    }

    getSocketSession(socket) {
    	let {Cookie} = FlowGRPCProxy.modules;
    	if(!Cookie){
    		this.log("Cookie module is required for socket session")
    		return Promise.reject("Cookie module is required for socket session");
    	}

        let cookies = null;
        try{
            cookies = this.unsignCookies(
        		Cookie.parse(socket.handshake.headers.cookie||''),
        		this.getHttpSessionSecret()
            );
        }catch(ex){
            this.log("Cookie.parse:error", ex);
            return Promise.reject(ex);
        }

        let sid = cookies[ this.getHttpSessionCookieName() ];

		return this.getSessionById(sid);
    }

	buildSessionOptions(){
		let sessionOptions = {
			secret: this.getHttpSessionSecret(),
			name: this.getHttpSessionCookieName(),
			resave: false,
			saveUninitialized:true,
			cookie: { maxAge: / *30 * 24 * * / 60 * 60 * 1000 } // 1 hour
		}
		this.emit("init::session-options", {options:sessionOptions})
		return sessionOptions
	}

	initSession(app){
		let {session} = FlowGRPCProxy.modules;
		let options = this.buildSessionOptions();
		if(options){
			this.sessionSecret = options.secret;
			this.sessionKey = options.name;
			if(!options.store)
				options.store = new session.MemoryStore();
			this.sessionStore = options.store;
			this.expressSession = session(options);
			app.use(this.expressSession);
		}
	}

    getSessionById(sid){
    	return new Promise((resolve, reject)=>{
	        if(!this.sessionStore)
	            return reject({error: "Session not initilized."});

	        this.sessionStore.get(sid, (err, session)=>{
	            if (err)
	                return reject(err);
	            if(!session)
	            	return reject(`${sid}: Session not found`)

	            session.id = sid;

	            resolve(session);
	        });
	    })
    }

	getHttpSessionCookieName(){
		let {session} = this.config.http || {};
		return (session && session.key)? session.key : 'connect.sid';
	}

	getHttpSessionSecret(secret="xyz"){
		if(this._httpSessionSecret)
			return this._httpSessionSecret;

		this._httpSessionSecret = crypto.createHash('sha256')
			.update(secret+this.config.http.session.secret)
			.digest('hex');
		return this._httpSessionSecret;
	}
	*/
}

module.exports = FlowGRPCProxy
