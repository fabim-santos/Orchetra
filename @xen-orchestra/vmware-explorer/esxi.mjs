import { Client } from '@vates/node-vsphere-soap'
import { createLogger } from '@xen-orchestra/log'
import { dirname } from 'node:path'
import { EventEmitter } from 'node:events'
import { strictEqual, notStrictEqual } from 'node:assert'
import fetch from 'node-fetch'
import https from 'https'

import parseVmdk from './parsers/vmdk.mjs'
import parseVmsd from './parsers/vmsd.mjs'
import parseVmx from './parsers/vmx.mjs'
import fs from 'node:fs/promises'

const { warn } = createLogger('xo:vmware-explorer:esxi')

export default class Esxi extends EventEmitter {
  #client
  #cookies
  #dcPath
  #host
  #httpsAgent
  #user
  #password
  #ready = false

  constructor(host, user, password, sslVerify) {
    super()
    this.#host = host
    this.#user = user
    this.#password = password
    if (!sslVerify) {
      this.#httpsAgent = new https.Agent({
        rejectUnauthorized: false,
      })
    }

    this.#client = new Client(host, user, password, sslVerify)
    this.#client.once('ready', async () => {
      try {
        // this.#ready is set to true to allow the this.search query to go through
        // this means that the server is connected and can answer API queries
        // you won't be able to download a file as long a the 'ready' event is not emitted
        this.#ready = true
        const res = await this.search('Datacenter', ['name'])
        this.#dcPath = Object.values(res)[0].name
        this.emit('ready')
      } catch (error) {
        this.emit('error', error)
      }
    })
    this.#client.on('error', err => {
      this.emit('error', err)
    })
  }

  #exec(cmd, args) {
    strictEqual(this.#ready, true)
    const client = this.#client
    return new Promise(function (resolve, reject) {
      client.once('error', function (error) {
        client.off('result', resolve)
        reject(error)
      })
      client.runCommand(cmd, args).once('result', function ( result, raw, soapHeader) {
        client.off('error', reject) 
        resolve(...arguments)
      })
    })
  }

  async #download(dataStore, path, range) {
    strictEqual(this.#ready, true)
    notStrictEqual(this.#dcPath, undefined)
    const url = new URL('https://localhost')
    url.host = this.#host
    url.pathname = '/folder/' + path
    url.searchParams.set('dcPath', this.#dcPath)
    url.searchParams.set('dsName', dataStore)
    const headers = {}
    if (this.#cookies) {
      headers.cookie = this.#cookies
    } else {
      headers.Authorization = 'Basic ' + Buffer.from(this.#user + ':' + this.#password).toString('base64')
    }
    if (range) {
      headers['content-type'] = 'multipart/byteranges'
      headers.Range = 'bytes=' + range
    }
    const res = await fetch(url, {
      agent: this.#httpsAgent,
      method: 'GET',
      headers,
      highWaterMark: 10 * 1024 * 1024,
    })
    if (res.status < 200 || res.status >= 300) {
      const error = new Error(res.status + ' ' + res.statusText + ' ' + url)
      error.cause = res
      throw error
    }
    if (res.headers.raw()['set-cookie']) {
      this.#cookies = res.headers
        .raw()
        ['set-cookie'].map(cookie => cookie.split(';')[0])
        .join('; ')
    }
    return res
  }

  async download(dataStore, path, range) {
    let tries = 5
    let lastError
    while (tries > 0) {
      try {
        const res = await this.#download(dataStore, path, range)
        return res
      } catch (error) {
        warn('got error , will retry in 2 seconds', { error })
        lastError = error
      }
      await new Promise(resolve => setTimeout(() => resolve(), 2000))
      tries--
    }

    throw lastError
  }

  // inspired from https://github.com/reedog117/node-vsphere-soap/blob/master/test/vsphere-soap.test.js#L95
  async search(type, properties) {
    // search types are limited to "ComputeResource", "Datacenter", "Datastore", "DistributedVirtualSwitch", "Folder", "HostSystem", "Network", "ResourcePool", "VirtualMachine"}
    // from https://github.com/vmware/govmomi/issues/2595#issuecomment-966604502
    // get property collector
    const propertyCollector = this.#client.serviceContent.propertyCollector
    // get view manager
    const viewManager = this.#client.serviceContent.viewManager
    // get root folder
    const rootFolder = this.#client.serviceContent.rootFolder
    let result = await this.#exec('CreateContainerView', {
      _this: viewManager,
      container: rootFolder,
      type: [type],
      recursive: true,
    })
    // build all the data structures needed to query all the vm names
    const containerView = result.returnval

    const objectSpec = {
      attributes: { 'xsi:type': 'ObjectSpec' }, // setting attributes xsi:type is important or else the server may mis-recognize types!
      obj: containerView,
      skip: true,
      selectSet: [
        {
          attributes: { 'xsi:type': 'TraversalSpec' },
          name: 'traverseEntities',
          type: 'ContainerView',
          path: 'view',
          skip: false,
        },
      ],
    }

    const propertyFilterSpec = {
      attributes: { 'xsi:type': 'PropertyFilterSpec' },
      propSet: properties.map(p => ({
        attributes: { 'xsi:type': 'PropertySpec' },
        type,
        pathSet: [p],
      })),
      objectSet: [objectSpec],
    }

    let token
    const objects = {}
    do {
      if (token !== undefined) {
        result = await this.#exec('ContinueRetrievePropertiesEx', {
          _this: propertyCollector,
          token,
        })
      } else {
        result = await this.#exec('RetrievePropertiesEx', {
          _this: propertyCollector,
          specSet: [propertyFilterSpec],
          options: { attributes: { type: 'RetrieveOptions' } },
        })
      }

      const returnObj = Array.isArray(result.returnval.objects) ? result.returnval.objects : [result.returnval.objects]
      returnObj.forEach(({ obj, propSet }) => {
        objects[obj.$value] = {}
        propSet = Array.isArray(propSet) ? propSet : [propSet]
        propSet.forEach(({ name, val }) => {
          // don't care about the type for now
          delete val.attributes
          // a scalar value : simplify it
          if (val.$value) {
            objects[obj.$value][name] = val.$value
          } else {
            objects[obj.$value][name] = val
          }
        })
      })

      token = result.returnval.token
    } while (token)

    return objects
  }

  async #inspectVmdk(dataStores, currentDataStore, currentPath, filePath) {
    let diskDataStore, diskPath
    if (filePath.startsWith('/')) {
      // disk is on another datastore
      Object.keys(dataStores).forEach(dataStoreUrl => {
        if (filePath.startsWith(dataStoreUrl)) {
          diskDataStore = dataStores[dataStoreUrl].name
          diskPath = filePath.substring(dataStoreUrl.length + 1)
        }
      })
    } else {
      diskDataStore = currentDataStore
      diskPath = currentPath + '/' + filePath
    }
    const vmdkRes = await this.download(diskDataStore, diskPath)
    const text = await vmdkRes.text()
    const parsed = parseVmdk(text)

    return {
      ...parsed,
      datastore: diskDataStore,
      path: dirname(diskPath),
      descriptionLabel: ' from esxi',
    }
  }

  async getAllVmMetadata() {
    const datas = await this.search('VirtualMachine', ['config', 'storage', 'runtime'])

    return Object.keys(datas).map(id => {
      const { config, storage, runtime } = datas[id]
      if (storage === undefined) {
        throw new Error(`source VM ${id} don't have any storage`)
      }
      const perDatastoreUsage = Array.isArray(storage.perDatastoreUsage)
        ? storage.perDatastoreUsage
        : [storage.perDatastoreUsage]
      return {
        id,
        nameLabel: config.name,
        memory: +config.hardware.memoryMB * 1024 * 1024,
        nCpus: +config.hardware.numCPU,
        guestToolsInstalled: false,
        firmware: config.firmware === 'efi' ? 'uefi' : config.firmware, // bios or uefi
        powerState: runtime.powerState,
        storage: perDatastoreUsage.reduce(
          (prev, curr) => {
            return {
              used: prev.used + +curr.committed,
              free: prev.free + +curr.uncommitted,
            }
          },
          { used: 0, free: 0 }
        ),
      }
    })
  }

  async getTransferableVmMetadata(vmId) {
    const search = await this.search('VirtualMachine', ['name', 'config', 'storage', 'runtime', 'snapshot'])
    if (search[vmId] === undefined) {
      throw new Error(`VM ${vmId} not found `)
    }
    const { config, runtime } = search[vmId]

    const [, dataStore, vmxPath] = config.files.vmPathName.match(/^\[(.*)\] (.+.vmx)$/)
    const res = await this.download(dataStore, vmxPath)
    const vmx = parseVmx(await res.text())
    // list datastores
    const dataStores = {}
    Object.values(await this.search('Datastore', ['summary'])).forEach(({ summary }) => {
      dataStores[summary.url] = summary
    })

    const disks = []
    const networks = []

    for (const key of Object.keys(vmx)) {
      const matches = key.match(/^(scsi|ide|ethernet)[0-9]+$/)
      if (matches === null) {
        continue
      }
      const channelType = matches[1]
      if (channelType === 'ide' || channelType === 'scsi') {
        const diskChannel = vmx[key]
        for (const diskIndex in Object.values(diskChannel)) {
          const disk = diskChannel[diskIndex]
          if (typeof disk !== 'object') {
            continue
          }
          if (disk.deviceType.match(/cdrom/i)) {
            continue
          }
          // can be something other than a disk, like a controller card
          if (channelType === 'scsi' && disk.deviceType !== 'scsi-hardDisk') {
            continue
          }

          disks.push({
            ...(await this.#inspectVmdk(dataStores, dataStore, dirname(vmxPath), disk.fileName)),
            node: `${key}:${diskIndex}`,
          })
        }
      } else if (channelType === 'ethernet') {
        const ethernet = vmx[key]

        networks.push({
          label: ethernet.networkName,
          macAddress: ethernet.generatedAddress,
          isGenerated: ethernet.addressType === 'generated',
        })
      }
    }
    let snapshots
    try {
      const vmsd = await (await this.download(dataStore, vmxPath.replace(/\.vmx$/, '.vmsd'))).text()
      snapshots = parseVmsd(vmsd)

      for (const snapshotIndex in snapshots?.snapshots) {
        const snapshot = snapshots.snapshots[snapshotIndex]
        for (const diskIndex in snapshot.disks) {
          const fileName = snapshot.disks[diskIndex].fileName
          snapshot.disks[diskIndex] = {
            node: snapshot.disks[diskIndex]?.node, // 'scsi0:0' , 'ide0:0', ...,
            ...(await this.#inspectVmdk(dataStores, dataStore, dirname(vmxPath), fileName)),
          }
        }
      }
    } catch (error) {
      // no vmsd file :fall back to a full withou snapshots
    }

    return {
      name_label: config.name,
      memory: +config.hardware.memoryMB * 1024 * 1024,
      nCpus: +config.hardware.numCPU,
      guestToolsInstalled: false,
      firmware: config.firmware === 'efi' ? 'uefi' : config.firmware, // bios or uefi
      powerState: runtime.powerState,
      snapshots,
      disks,
      networks,
    }
  }

  powerOff(vmId) {
    return this.#exec('PowerOffVM_Task', { _this: vmId })
  }
  powerOn(vmId) {
    return this.#exec('PowerOnVM_Task', { _this: vmId })
  }

  async fetchProperty(type, id, propertyName){
    // the fetch method does not seems to be exposed by the wsdl
    // inpired  by the pyvmomi implementation
    const res = await fetch(`https://${this.#host}/sdk`,{
        method:'POST',
        headers: {
          Cookie: this.#client.authCookie.cookies,
        },
        agent: this.#httpsAgent,
        body:`<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope 
          xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/" 
          xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        >
          <soapenv:Body>
            <Fetch xmlns="urn:vim25">
              <_this type="${type}">${id}</_this>
              <prop >${propertyName}</prop>
            </Fetch>
          </soapenv:Body>
        </soapenv:Envelope>`
      })
      const text = await res.text()
      const response = text.match(/<returnval[^>]*>(.*)<\/returnval>/)[1]

      return response
  }

  async export(vmId){
    const exported = await  this.#exec('ExportVm', { _this: vmId }) 
    const exportTaskId  = exported.returnval.$value 
    let isReady = false
    for(let i=0; i <10 && !isReady; i ++){
      const state = await this.fetchProperty('HttpNfcLease',exportTaskId,  'state')
      isReady = state ==='ready'
      if(!isReady){
        await new Promise(resolve=>setTimeout(resolve, 1000)) 
      }
    }
  
    if(!isReady){
      throw new Error('not ready')
    }

    const info = await this.fetchProperty('HttpNfcLease',exportTaskId,  'info') 
    const matches = info.matchAll( /<url>(.+\.vmdk)<\/url>/g)
    for(const match of matches){
      const vmdkUrl = match[1]
      const url = vmdkUrl.replace('https://*/', `https://${this.#host}/`)
      const vmdkres = await fetch(
        url,
        {
          rejectUnauthorized: false,
        agent: this.#httpsAgent,
        Cookie: this.#client.authCookie.cookies,
      })
      console.log(vmdkres)
      const stream = vmdkres.body
      let total = 0
      const start = Date.now()
      // const fd = await fs.open('/tmp/out.vmdk', 'w')
      for await(const buffer of stream ){
        total+= buffer.length
        console.log(buffer.length, total, Math.round(total/1024 *1000/ (Date.now() - start)))
        // await fd.write(buffer)
      }
      console.log('done')
    }
   
    return exported
  }
}