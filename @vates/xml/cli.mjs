#!/usr/bin/env node

import { inspect } from 'node:util'
import { format } from '@vates/xml/format.mjs'
import { parse } from '@vates/xml/parse.mjs'
import { readFileSync } from 'node:fs'

function log(val) {
  process.stdout.write(inspect(val, false, null, true))
  process.stdout.write('\n')
}

function main([inputPath = 0]) {
  const input = readFileSync(inputPath)

  // attempt to parse from JSON
  let data
  try {
    data = JSON.parse(input)
  } catch (error) {
    // fallback to XML
    log(parse(input))
    return
  }

  process.stdout.write(format(data))
}
main(process.argv.slice(2))
