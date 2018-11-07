# mysql_mongoose
a mysql_client js library, and mongoose like

test:

git clone https://github.com/zswDev/mysql_mongoose.git

```javascript
const db = requrie('mysql_mongoose')
let [err, rows] = db.find('user',{
    id: {$gt: 1},
    $or: [
        {
            id: {
                $in: [1, 2, 3]
            }
        },
        {
            id: 4    
        }
    ]
},
'id username',
{
    $sort: {id: -1},
    $limit: 10
})
// select id,username from user where id>1 and (id in(1,2,3) or id=1) order by id desc limit 10
```