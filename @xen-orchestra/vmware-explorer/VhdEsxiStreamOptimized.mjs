import { VhdAbstract } from 'vhd-lib'
import { unpackFooter, unpackHeader } from 'vhd-lib/Vhd/_utils.js'
import _computeGeometryForSize from 'vhd-lib/_computeGeometryForSize.js'
import { DISK_TYPES, FOOTER_SIZE, SECTOR_SIZE } from 'vhd-lib/_constants.js'
import { createFooter, createHeader } from 'vhd-lib/_createFooterHeader.js'
import { unpackHeader as unpackVmdkHeader } from 'xo-vmdk-to-vhd/dist/definitions.js'

import zlib from 'zlib'
import assert from 'node:assert'

export default class VhdEsxiStreamOptimized extends VhdAbstract {
  #handler
  #vmdkHeader
  #l1table
  #path

  get header() {
    // a grain directory entry contains the address of a grain table
    // a grain table can adresses at most 4096 grain of 512 Bytes of data
    return unpackHeader(createHeader(Math.ceil(this.#vmdkHeader.capacitySectors * SECTOR_SIZE)))
  }

  get footer() {
    const size = this.#vmdkHeader.capacitySectors * SECTOR_SIZE
    const geometry = _computeGeometryForSize(size)
    return unpackFooter(createFooter(size, Math.floor(Date.now() / 1000), geometry, FOOTER_SIZE, DISK_TYPES.DYNAMIC))
  }

  constructor(handler, path) {
    super()
    this.#handler = handler
    this.#path = path
  }
  async readHeaderAndFooter() {
    // complete header is at the end of the file
    // the header at the beginning don't have the L1 table position
    const size = await this.#handler.getSize(this.#path)
    const headerBuffer = await this.#read(size - 1024, 1024)
    this.#vmdkHeader = unpackVmdkHeader(headerBuffer)
  }
  async readBlockAllocationTable() {
    const l1entries = Math.floor(
      (this.#vmdkHeader.capacitySectors + this.#vmdkHeader.l1EntrySectors - 1) / this.#vmdkHeader.l1EntrySectors
    )
    this.#l1table = await this.#read(this.#vmdkHeader.grainDirectoryOffsetSectors * SECTOR_SIZE, l1entries * 4)
  }
  async #read(start, length) {
    const buffer = Buffer.alloc(length, 0)
    await this.#handler.read(this.#path, buffer, start)
    return buffer
  }
  async #getGrainTable(grainTableIndex) {
    const l1Entry = this.#l1table.readUInt32LE(grainTableIndex * 4)
    if (l1Entry === 0) {
      return
    }
    const l2ByteSize = this.#vmdkHeader.numGTEsPerGT * 4
    return this.#read(l1Entry * SECTOR_SIZE, l2ByteSize)
  }
  async #getGrainData(grainTableIndex, grainEntryIndex) {
    const grainTable = this.#getGrainTable(grainTableIndex)
    if (grainTable === undefined) {
      return
    }
    const offset = grainTable[grainEntryIndex]
    if (offset === 0) {
      return
    }
    const dataSizeBuffer = this.#read(offset * SECTOR_SIZE + 8, 4)
    const dataSize = dataSizeBuffer.readUInt32LE()
    const buffer = await this.#read(offset * SECTOR_SIZE + 16, dataSize)
    const inflated = zlib.inflateSync(buffer)
    assert(inflated.length, 64 * 1024)
    return inflated
  }
  async readBlock(blockId) {
    // 1 grain = 128  sectors = 64KB
    // 512 grain per grain table => 32MB per grain table, 8 vhd blocks
    const buffer = Buffer.alloc(512 /* bitmap */ + 2 * 1024 * 1024 /* data */, 0)

    const grainTableIndex = Math.floor(blockId / 8)
    for (let i = 0; i < 8; i++) {
      const grainEntryIndex = (blockId % 8) + i
      const grain = await this.#getGrainData(grainTableIndex, grainEntryIndex)
      if (buffer !== undefined) {
        grain.copy(buffer, i * 128 * 512)
      }
    }
    return {
      id: blockId,
      bitmap: buffer.slice(0, 512),
      data: buffer.slice(512),
      buffer,
    }
  }
}
