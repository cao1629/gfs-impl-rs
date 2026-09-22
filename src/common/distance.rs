use std::net::Ipv4Addr;

use crate::rpc::Replica;

#[derive(Clone, Debug, Default)]
pub struct Endpoint {
    pub address: String,
    pub rack: String,
}

impl Endpoint {
    pub fn of(replica: &Replica) -> Endpoint {
        Endpoint { address: replica.address.clone(), rack: replica.rack.clone() }
    }
}

fn parse_ipv4(address: &str) -> Option<u32> {
    let host = match address.rfind(':') {
        Some(colon) => &address[..colon],
        None => address,
    };
    host.parse::<Ipv4Addr>().ok().map(u32::from)
}

pub fn distance_between(a: &Endpoint, b: &Endpoint) -> u32 {
    if !a.rack.is_empty() && !b.rack.is_empty() {
        return if a.rack == b.rack { 0 } else { 1 };
    }
    match (parse_ipv4(&a.address), parse_ipv4(&b.address)) {
        (Some(x), Some(y)) => 32 - (x ^ y).leading_zeros(),
        _ => 32,
    }
}

pub fn order_push_chain(origin: &Endpoint, mut replicas: Vec<Replica>) -> Vec<Replica> {
    let mut chain = Vec::with_capacity(replicas.len());
    let mut current = origin.clone();
    while !replicas.is_empty() {
        let mut best = 0;
        let mut best_distance = u32::MAX;
        for (i, replica) in replicas.iter().enumerate() {
            let distance = distance_between(&current, &Endpoint::of(replica));
            if distance < best_distance {
                best_distance = distance;
                best = i;
            }
        }
        let next = replicas.remove(best);
        current = Endpoint::of(&next);
        chain.push(next);
    }
    chain
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(address: &str, rack: &str) -> Endpoint {
        Endpoint { address: address.to_string(), rack: rack.to_string() }
    }

    fn replica(id: &str, address: &str, rack: &str) -> Replica {
        Replica { chunkserver_id: id.to_string(), address: address.to_string(), rack: rack.to_string() }
    }

    #[test]
    fn rack_labels_win_when_present() {
        assert_eq!(distance_between(&endpoint("10.0.0.1:1", "r1"), &endpoint("10.0.0.2:1", "r1")), 0);
        assert_eq!(distance_between(&endpoint("10.0.0.1:1", "r1"), &endpoint("10.0.0.2:1", "r2")), 1);
    }

    #[test]
    fn ip_prefix_when_no_labels() {
        assert_eq!(distance_between(&endpoint("10.0.0.1:1", ""), &endpoint("10.0.0.2:1", "")), 2);
        assert_eq!(distance_between(&endpoint("10.0.0.1:1", ""), &endpoint("10.1.0.1:1", "")), 17);
        assert_eq!(distance_between(&endpoint("", ""), &endpoint("10.0.0.1:1", "")), 32);
    }

    #[test]
    fn chain_visits_nearest_first() {
        let replicas = vec![replica("a", "10.1.0.1:1", ""), replica("b", "10.0.0.9:1", ""), replica("c", "10.0.0.2:1", "")];
        let chain = order_push_chain(&endpoint("10.0.0.1:0", ""), replicas);
        let ids: Vec<&str> = chain.iter().map(|r| r.chunkserver_id.as_str()).collect();
        assert_eq!(ids, vec!["c", "b", "a"]);
    }

    #[test]
    fn degenerates_to_a_chain_not_a_star() {
        let replicas = vec![replica("a", "127.0.0.1:1", ""), replica("b", "127.0.0.1:2", ""), replica("c", "127.0.0.1:3", "")];
        let chain = order_push_chain(&endpoint("", ""), replicas);
        assert_eq!(chain.len(), 3);
    }
}
