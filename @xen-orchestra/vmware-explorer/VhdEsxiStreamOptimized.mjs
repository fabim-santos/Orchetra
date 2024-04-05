import { VhdAbstract } from 'vhd-lib'

export default class VhdEsxiStreamOptimized extends VhdAbstract {
  #grainTableIndexes = {}

  async readHeaderAndFooter() {}
  async readBlockAllocationTable() {
    // will need to read the full export
    // since the L1 table is after all the blocks exports
    // read all the grain table by checking the markers
    // hoping they are chained (to the rythm)
    // build a BAT that will produce the block in the stream order
  }
  async #read(start, length) {}
  async #getGrainTable(grainTableIndex) {}
  async #getGrain(grainTableIndex, grainEntryIndex) {
    const grainTable = this.#getGrainTable(grainTableIndex)
    const offset = grainTable[grainEntryIndex]
    if (offset === 0) {
      return undefined
    }
    return this.#read(offset * 128 * 512, 128 * 512)
  }
  async readBlock(blockId) {
    // 1 grain = 128  sectors = 64KB
    // 512 grain per grain table => 32MB per grain table, 8 vhd blocks
    const buffer = Buffer.alloc(512 /* bitmap */ + 2 * 1024 * 1024 /* data */, 0)

    const grainTableIndex = Math.floor(blockId / 8)
    for (let i = 0; i < 8; i++) {
      const grainEntryIndex = (blockId % 8) + i
      const grain = await this.#getGrain(grainTableIndex, grainEntryIndex)
      if (buffer !== undefined) {
        grain.copy(buffer, i * 128 * 512)
      }
    }
  }
}

/* {
  magicString: 'KDMV',
  version: 3,
  flags: {
    newLineTest: true,
    useSecondaryGrain: false,
    useZeroedGrainTable: false,
    compressedGrains: true,
    hasMarkers: true
  },
  compressionMethod: 'COMPRESSION_DEFLATE',
  grainSizeSectors: 128,
  overheadSectors: 128,
  capacitySectors: 16777216,
  descriptorOffsetSectors: 1,
  descriptorSizeSectors: 1,
  grainDirectoryOffsetSectors: 3249279,
  rGrainDirectoryOffsetSectors: 0,
  l1EntrySectors: 65536,
  numGTEsPerGT: 512
}
*/
