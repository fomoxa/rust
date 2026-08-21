use cyclone_attributes::*;

#[network]
#[codec(state, input)]
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Vector3 {
    #[network(f32)]
    #[codec(state, input)]
    pub x: f32,

    #[network(f32)]
    #[codec(state, input)]
    pub y: f32,

    #[network(f32)]
    #[codec(state, input)]
    pub z: f32,
}

#[network]
#[codec(state)]
#[derive(Debug, Default, Clone, PartialEq)]
pub struct GameMessage {
    #[network(u32)]
    #[codec(state)]
    pub player_id: u32,

    #[network(string)]
    #[codec(state)]
    pub player_name: String,

    #[network(Vector3)]
    #[codec(state)]
    pub position: Vector3,

    #[network(u32)]
    #[codec(state)]
    pub health: u32,

    #[network(bool)]
    #[codec(state)]
    pub is_alive: bool,

    pub last_seen_at: u64,
}

#[network]
#[codec(input)]
#[derive(Debug, Default, Clone, PartialEq)]
pub struct PlayerInput {
    #[network(u64)]
    #[codec(input)]
    pub tick: u64,

    #[network(Vector3)]
    #[codec(input)]
    pub direction: Vector3,

    #[network(bool)]
    #[codec(input)]
    pub firing: bool,
}
