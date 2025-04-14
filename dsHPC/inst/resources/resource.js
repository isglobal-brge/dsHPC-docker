var dsHPC = {
  settings: {
    title: "High-Performance Computing API Resources",
    description:
      "Provides access to DataSHIELD's High-Performance Computing API endpoints.",
    web: "https://github.com/isglobal-brge/dsHPC",
    categories: [
      {
        name: "dshpc-api",
        title: "dsHPC API",
        description:
          'The resource connects to a <a href="https://github.com/isglobal-brge/dsHPC" target="_blank">dsHPC</a> API endpoint.',
      },
    ],
    types: [
      {
        name: "dshpc-api-endpoint",
        title: "dsHPC API Endpoint",
        description:
          'Connection to a dsHPC API endpoint.',
        tags: ["dshpc-api"],
        parameters: {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "host",
              "type": "string",
              "title": "Host name or IP address",
              "description": "The hostname or IP address of the dsHPC API"
            },
            {
              "key": "port",
              "type": "integer",
              "title": "Port number",
              "description": "The port number of the dsHPC API"
            }
          ],
          "required": ["host", "port"],
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "apikey",
              "type": "string",
              "title": "API Key",
              "format": "password",
              "description": "The API key for authentication"
            }
          ],
          "required": ["apikey"],
        },
      },
    ],
  },
  asResource: function (type, name, params, credentials) {
    var toHPCResource = function(name, params, credentials) {
      return {
        name: name,
        url: "http://" + params.host + ":" + params.port,
        format: "dshpc.api",
        identity: "",
        secret: credentials.apikey
      };
    };
    
    var toResourceFactories = {
      "dshpc-api-endpoint": toHPCResource
    };
    
    if (toResourceFactories[type]) {
      return toResourceFactories[type](name, params, credentials);
    }
    return undefined;
  },
};
