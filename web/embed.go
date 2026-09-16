// Package web embeds the demo SPA into the api binary — a single deployable
// artifact with no separate frontend hosting (CONTRACTS §9 demo topology).
package web

import "embed"

//go:embed index.html style.css app.js
var FS embed.FS
