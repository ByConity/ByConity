const fs = require('fs')

const SOURCE_ROOT = __dirname + '/../source/_posts'

const langList = fs.readdirSync(SOURCE_ROOT)

langList.map((lang) => {
    const contentTypeList = fs.readdirSync(`${SOURCE_ROOT}/${lang}`)
    contentTypeList.map((contentType) => {
        const res = getFileList(`${SOURCE_ROOT}/${lang}/${contentType}`)
        // console.log('res = ', res)

        // Generate
        let result = 
`---
title: ${contentType.toUpperCase()}
---
`
        for (let item of res) {
            if (item.children.length) {
                result += `\n\n## ${item.name}\n`
                const children = getItemListString(item.children, `/${lang}/${contentType}/${item.name}`)
                result += children.join('\n')
            }
        }

        result += '\n'
        // console.log('result = ', result)

        // Save it
        fs.writeFileSync(`${SOURCE_ROOT}/../${lang}/${contentType}/index.md`, result)
    })
})


function getFileList(path) {
    if (/\.DS_Store$|\.md$/.test(path)) {
        return []
    }
    try {
        const fileList = fs.readdirSync(path)
        const result = []
        fileList.map(f => {
            if (/\.DS_Store$/.test(f)) {
                return
            }
            result.push({
                name: f,
                children: getFileList(`${path}/${f}`)
            })
        })
        return result
    } catch(e) {
        console.log('e = ', e)
        return []
    }
}

function getItemListString (arr, parentPath, indent) {
    let result = []
    parentPath = parentPath ? parentPath : ''
    const INDENT_SIGN = '  '
    indent = indent || ''

    for (let i = 0; i < arr.length; i++) {
        const item = arr[i]
        if (item.children.length) {
            const children = getItemListString(item.children, `${parentPath}/${item.name}`, INDENT_SIGN + indent)
            result = result.concat(children)
        } else {
            // removed file extension
            const displayName = item.name.replace(/\.md$/, '')
            result.push(`${indent}${i+1}. [${displayName}](${encodeURIComponent(parentPath + '/' + displayName)})`)
        }
    }
    return result
}
