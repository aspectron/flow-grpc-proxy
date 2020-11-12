const FlowGRPCProxy = require("./lib/flow-grpc-proxy.js");

module.exports = (modules)=>{
	//let {express} = modules||{};
	//if(typeof express!= 'function')
	//	throw new Error("flow-grpc-proxy.FlowGRPCProxy requires express module.");
	FlowGRPCProxy.modules = modules;
	return {FlowGRPCProxy}
}
