const fs = require('fs')
const zlib = require('zlib')
const readline = require('readline')
const moment = require('moment')
const db = require('./mongodb')

const dir = '/amazon_data/blueplanet'
// const filenames = fs.readdirSync(dir).map(filename => `${dir}/${filename}`)
const filenames = ['./testdata.gz', './testdata2.gz']
// const filenames = [`${dir}/blueplanet-20190304.gz`, `${dir}/blueplanet-20190305.gz`, `${dir}/blueplanet-20190306.gz`]

let topicTrends = {}
let count = 0

const calcTrends = async (filename) => {
  const gzFileInput = fs.createReadStream(filename)
  const gunzip = zlib.createGunzip()

  const interface = readline.createInterface({
    input: gunzip,
  })

  trends = await new Promise((resolve, reject) => {
    console.log(moment().format(), `Start reading ${filename}`)
    // let _topicTrends = {}
    let _tagTrends = []
    interface.on('line', line => {
      if (!line.includes('start'))
        return
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
        topicTrends[topicId] = { topic_id: topicId, updated_time: event.updated_time, rooms: event.rooms, tags: event.tags, viewer: 1, mids: event.mid && event.mid !== '0' ? [event.mid] : [], tcs: event.tc && event.tc !== '0' ? [event.tc] : [] }
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
      resolve({ tagTrends: _tagTrends })
    })
    gzFileInput.on('data', function (data) {
      gunzip.write(data)
    })
    gzFileInput.on('end', function () {
      gunzip.end()
    })
  })
  count++
  console.log(moment().format(), `Done reading ${filename} (${count}/${filenames.length})`)
  return trends
}

// -------------------------------------- main --------------------------------------

const main = async () => {
  const startTime = moment().format()
  console.log(startTime, 'Start')
  let result = `${startTime}, Start`

  const tagTrends = []

  for (const filename of filenames) {
    const _trends = await calcTrends(filename)
    console.log(`There are ${_trends.tagTrends.length} tags and ${Object.keys(topicTrends).length} topics`)

    if (!tagTrends) {
      tagTrends = _trends.tagTrends
      continue
    }
    _trends.tagTrends.forEach(tagTrend => {
      const findTagTrend = tagTrends.find(t => t.tag === tagTrend.tag)
      if (!findTagTrend) {
        tagTrends.push(tagTrend)
        return
      }
      findTagTrend.viewer = (findTagTrend.viewer || 0) + (tagTrend.viewer || 0)
    })
    db.get('Events').insert(_trends.tagTrends)
    console.log(moment().format(), `Done calculating topic and tag trends for ${filename}.`)
  }

  console.log('\n----------------------\n')
  topicTrends = Object.values(topicTrends)
  topicTrends.sort((a, b) => b.viewer - a.viewer)
  tagTrends.sort((a, b) => b.viewer - a.viewer)

  /* Start of tag trend */

  // const topMostPopularTags = 'Top most popular tags'
  // console.log(topMostPopularTags)
  // result += '\n\n' + topMostPopularTags
  // for (let i = 0; i < tagTrends.length; i++) {
  //   const { tag, viewer, topics } = tagTrends[i]
  //   const topTagString = `${i + 1}.) ${tag}, from ${viewer} viewers in ${topics.length} topics`
  //   console.log(topTagString)
  //   result += '\n' + topTagString
  // }

  /* End of tag trend */

  const topMostPopularTopics = 'Top most popular topics'
  console.log(topMostPopularTopics)
  result += '\n\n' + topMostPopularTopics
  for (let i = 0; i < 10; i++) {
    const { topic_id, viewer, rooms } = topicTrends[i]
    const topTopicString = `${i + 1}.) ${topic_id}, from ${viewer} viewers from [${rooms}] rooms.`
    console.log(topTopicString)
    result += '\n' + topTopicString
  }

  const endTime = moment()
  const endString = `\n${endTime.format()} Done. Running time: ${endTime.diff(startTime, 'seconds')} seconds.`
  console.log(endString)
  result += '\n' + endString + '\n----------------------\n'

  fs.appendFileSync('./result.txt', result)
}

main()
