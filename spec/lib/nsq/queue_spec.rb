describe ::Queue, focus: true do
  it_behaves_like 'a thread-safe queue'
end

describe Nsq::BasicQueue, focus: true do
  it_behaves_like 'a thread-safe queue'
end

describe Nsq::LockFreeQueue, focus: true do
  it_behaves_like 'a thread-safe queue'
end