mod pb;

use pb::uniswap_v1::{Swap, Swaps};
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_ethereum::Event;
use substreams::Hex;
use hex_literal::hex;

// Uniswap V3 Swap event signature
// event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
const SWAP_TOPIC: [u8; 32] = hex!("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67");

#[substreams::handlers::map]
fn map_swaps(block: eth::Block) -> Result<Swaps, substreams::errors::Error> {
    let mut swaps = vec![];

    for log in block.logs() {
        if log.topics().is_empty() {
            continue;
        }

        if log.topics()[0] == SWAP_TOPIC {
            // Very simple extraction to match the structure
            // In a real module, use abi encoding, but this demonstrates the filter
            let tx_hash = Hex::encode(&log.receipt.transaction.hash);
            let pool_address = Hex::encode(&log.address());

            // Note: properly decoding the ABI requires generating abi files and using ethabi.
            // For now, we populate the proto with the basic info and strings for the data.
            swaps.push(Swap {
                tx_hash,
                block_number: block.number,
                timestamp: block.timestamp_seconds(),
                pool_address,
                sender: Hex::encode(&log.topics().get(1).unwrap_or(&vec![])),
                recipient: Hex::encode(&log.topics().get(2).unwrap_or(&vec![])),
                amount0: Hex::encode(&log.data()[0..32]), // just placeholder hex, should be abi decoded
                amount1: Hex::encode(&log.data()[32..64]),
                sqrt_price_x96: Hex::encode(&log.data()[64..96]),
                liquidity: Hex::encode(&log.data()[96..128]),
                tick: 0, // placeholder
            });
        }
    }

    Ok(Swaps { swaps })
}
