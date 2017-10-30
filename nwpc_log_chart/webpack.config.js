'use strict';
let path = require('path');

let module_config= {
  rules: [
    {
      test: /\.js$/,
      use: [
        "babel-loader"
      ],
      exclude: /node_modules/,
      include: __dirname
    },
    {
      test: /\.css/,
      use: [
        "css-loader",
      ]
    }
  ]
};

module.exports = {
  entry: './src/index.js',
  output: {
    path: path.join(__dirname, 'dist'),
    filename: 'bundle.js',
  },
  module: module_config,
  target: 'node'
};