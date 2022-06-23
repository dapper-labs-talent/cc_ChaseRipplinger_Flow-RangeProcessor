# Range request processing problem

Each new node which joins a blockchain network starts from a blank slate (i.e. at block height 0), and must catch up to the latest height of the chain by downloading past blocks from other nodes on the network. It does this using a synchronization engine, which sends requests for ranges of blocks to various nodes on the network and tracks the responses.

Because a blockchain network is a “trustless” network, we cannot blindly accept the responses we receive. To increase the likelihood that we receive the correct block for each height, we consider a given height to be **fulfilled** only once we've received at least `n` responses containing that height. Note that we do not require these `n` responses to actually agree on the contents of the block, only that there are at least `n` of them in total.

### Task:

The task for this assignment is to implement a `RangeResponseProcessor` to help track the range of block heights that the synchronization engine should actively be requesting. We will refer to this as the **active range**. The `RangeResponseProcessor` should have the following methods:

- `ProcessRange(startHeight uint64, blocks []Block)`
    - This is called by the synchronization engine to process each range response it receives.
    - You may assume that the input represents a consecutive range of blocks starting at height `startHeight`. For example, the first element of `blocks` has height `startHeight`, the second element has height `startHeight + 1`, and so on.
  - ❗️You must handle arbitrary inputs (i.e. do not make any assumptions about the value of `startHeight` or the size of `blocks`). Any blocks in the input that fall outside the current active range should be ignored. The definition of the current active range is given below.
- `GetActiveRange() (minHeight uint64, maxHeight uint64)`
  - This should return the current active range as a tuple `(h, h+s-1)`, where `h` is the minimum height such that fewer than `n` range responses containing height `h` have been received so far. In other words, `h` is the minimum height that has yet to be fulfilled. 
  - `s` represents the size of the active range. The size of the active range is constant.
  - `s` and `n` are parameters that should be provided to the `RangeResponseProcessor` upon instantiation.
  - ❗️Consider efficiency in your solution, response rates from node peers will be high and optimal time complexity is essential.

For simplicity, you may assume that the underlying representation of a `Block` is simply a string:

```go
type Block string
```

## General guidance, please read carefully

- Do not write low-level, networking or other IPC, multi-process or container centric solutions. This is a local development coding test meant to only take 1-2 hours maximum.
- We prefer candidates to use GoLang which Flow is built using, however, please feel free to use any language of your preference if needed.
- ❗You should ensure that your implementation is concurrency-safe.
- ❗Please approach this as you would a real-world production problem. Apply the same quality expectations as you would when submitting an ideal PR to your own team.
- ❗When submitting: please include any necessary details on the implementation, assumptions, time complexity or other relevant info to share.
- If anything remains unclear about this problem don’t hesitate to ask your Talent team associate who can get follow-ups from engineering.
