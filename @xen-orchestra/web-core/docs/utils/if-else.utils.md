# `ifElse`

`ifElse` utility is a wrapper around `watch`.

It allows you to watch a `boolean` source and execute the first callback if the source is `true`, otherwise the second
callback is executed.

```ts
ifElse(mySource, doSomething, doSomethingElse)
ifElse(mySource, doSomething, doSomethingElse, { immediate: true })
```

## Example

```ts
const myValue = ref(5)

ifElse(
  () => myValue.value > 10,
  () => console.log('myValue is greater than 10'),
  () => console.log('myValue is less than or equal to 10')
)
```
