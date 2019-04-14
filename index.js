const fs = require('fs')
const zlib = require('zlib')
const readline = require('readline')
const moment = require('moment')
const async = require('async')

const dir = '/amazon_data/blueplanet'
// const filenames = fs.readdirSync(dir).map(filename => `${dir}/${filename}`)
const filenames = ['./testdata.gz', './testdata2.gz']
// const filenames = [`${dir}/blueplanet-20181015.gz`, `${dir}/blueplanet-20181016.gz`]

let topicTrends = {}
let count = 0

const calcTagTrends = async (filename) => {
  const gzFileInput = fs.createReadStream(filename)
  const gunzip = zlib.createGunzip()

  const interface = readline.createInterface({
    input: gunzip,
  })

  tagTrends = await new Promise((resolve, reject) => {
    console.log(moment().format(), `Start reading ${filename}`)
    // topicTrends = {}
    // const __events = []
    let _tagTrends = []
    interface.on('line', line => {
      if (!line.includes('start'))
        return
      // __events.push(JSON.parse(line))
      const event = JSON.parse(line)

      const topicId = event.topic_id
      let { tags = [] } = event
      tags = Object.values(tags)
      if (event.event !== 'start' || topicId === 0 || topicId === '0')
        return
      if (topicTrends[topicId] !== undefined) {
        if (!(event.mid === '0' || event.mid === 0) && !topicTrends[topicId].mids.includes(event.mid)) {
          topicTrends[topicId].viewer++
          topicTrends[topicId].mids.push(event.mid)
          if (!tags || tags === undefined || typeof tags !== 'object' || tags.length < 1)
            return
          try {
            tags.forEach(tag => {
              const tagFind = _tagTrends.find(t => t.tag === tag)
              if (tagFind) {
                tagFind.viewer++
                if (!tagFind.topics.includes(topicId))
                  tagFind.topics.push(topicId)
              } else {
                _tagTrends.push({ tag, viewer: 1, topics: [topicId] })
              }
            })
          } catch (err) {

          }
        } else if (!(event.tc === '0' || event.tc === 0) && !topicTrends[topicId].tcs.includes(event.tc)) {
          topicTrends[topicId].viewer++
          topicTrends[topicId].mids.push(event.tc)
          if (!tags || tags === undefined || typeof tags !== 'object' || tags.length < 1)
            return
          try {
            tags.forEach(tag => {
              const tagFind = _tagTrends.find(t => t.tag === tag)
              if (tagFind) {
                tagFind.viewer++
                if (!tagFind.topics.includes(topicId))
                  tagFind.topics.push(topicId)
              } else {
                _tagTrends.push({ tag, viewer: 1, topics: [topicId] })
              }
            })
          } catch (err) {

          }
        }
      } else {
        topicTrends[topicId] = { topic_id: topicId, score: 1, mids: event.mid && event.mid !== '0' ? [event.mid] : [], tcs: event.tc && event.tc !== '0' ? [event.tc] : [] }
        if (!tags || tags === undefined || typeof tags !== 'object' || tags.length < 1)
          return
        try {
          tags.forEach(tag => {
            const tagFind = _tagTrends.find(t => t.tag === tag)
            if (tagFind) {
              tagFind.viewer++
              if (!tagFind.topics.includes(topicId))
                tagFind.topics.push(topicId)
            } else {
              _tagTrends.push({ tag, viewer: 1, topics: [topicId] })
            }
          })
        } catch (err) {

        }
      }

    })
    interface.on('error', (err) => { reject(err) })
    interface.on('close', () => {
      // resolve(__events)
      resolve(_tagTrends)
    })
    gzFileInput.on('data', function (data) {
      gunzip.write(data)
    })
    gzFileInput.on('end', function () {
      gunzip.end()
    })
  })
  // events.push(..._events)
  count++
  console.log(moment().format(), `Done reading ${filename} (${count}/${filenames.length})`)
  return tagTrends
}

const main = async () => {
  const startTime = moment().format()
  console.log(startTime, 'Start')
  let result = `${startTime}, Start`

  // const events = []
  let tagTrends = []

  const _filenames = []
  const chunkSize = 2
  let index = 0
  for (let i = 0; i < filenames.length; i++) {
    if (!_filenames[index])
      _filenames[index] = []
    _filenames[index].push(filenames[i])
    if ((i + 1) % chunkSize === 0)
      index++
  }
  for (const chunk of _filenames) {
    // await Promise.all(chunk.map(_filename => calcTagTrends(_filename)))
    // .then(values => {
    // async.parallel(chunk.map(_filename => calcTagTrends(_filename)), (err, values) => {
    async.map(chunk, calcTagTrends, (err, values) => {
      if (err) throw err
      values.forEach(_tagTrends => {
        if (!tagTrends) {
          tagTrends = _tagTrends
          return
        }
        _tagTrends.forEach(tagTrend => {
          const findTagTrend = tagTrends.find(t => t.tag === tagTrend.tag)
          if (!findTagTrend) {
            tagTrends.push(tagTrend)
            return
          }
          findTagTrend.viewer = (findTagTrend.viewer || 0) + (tagTrend.viewer || 0)
        })
      })
    })
  }


  // for (const filename of filenames) {
  //   const _tagTrends = await calcTagTrends(filename)
  //   if (!tagTrends) {
  //     tagTrends = _tagTrends
  //     continue
  //   }
  //   _tagTrends.forEach(tagTrend => {
  //     const findTagTrend = tagTrends.find(t => t.tag === tagTrend.tag)
  //     if (!findTagTrend) {
  //       tagTrends.push(tagTrend)
  //       return
  //     }
  //     findTagTrend.viewer = (findTagTrend.viewer || 0) + (tagTrend.viewer || 0)
  //   })
  // }

  console.log('\n----------------------\n')
  // topicTrends = Object.values(topicTrends)
  tagTrends.sort((a, b) => b.viewer - a.viewer)

  const topMostPopularTags = 'Top most popular tags'
  console.log(topMostPopularTags)
  result += '\n\n' + topMostPopularTags
  for (let i = 0; i < tagTrends.length; i++) {
    const { tag, viewer, topics } = tagTrends[i]
    const topTagString = `${i + 1}.) ${tag}, from ${viewer} viewers in ${topics.length} topics`
    console.log(topTagString)
    result += '\n' + topTagString
  }

  const endTime = moment()
  const endString = `\n${endTime.format()} Done. Running time: ${endTime.diff(startTime, 'seconds')} seconds.`
  console.log(endString)
  result += '\n' + endString + '\n----------------------\n'

  fs.appendFileSync('./result.txt', result)
}

main()
